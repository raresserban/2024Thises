# Databricks notebook source
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow  
import mlflow.spark  
from mlflow.tracking import MlflowClient
import plotly.graph_objects as go
import smtplib  
import json  
from email.mime.multipart import MIMEMultipart  
from email.mime.text import MIMEText  
import logging
import metadata

accountkey = dbutils.secrets.get(scope = 'landingzone' , key = 'container' )
spark.conf.set("fs.azure.account.key.landingzone6291.blob.core.windows.net" , accountkey)

def import_dataframe_from_version(
    import_table_name: str = "",
    import_table_catalog_path: str = "",
    from_version = None,
    to_version = None,
) -> DataFrame:
    import_table_path = import_table_catalog_path + import_table_name

    # Assertions:
    if not spark.catalog.tableExists(import_table_path):
        raise ValueError(f"Table {import_table_path} does not exist.")

    # Assertion if the enableChangeDataFeed was not enabled
    assert metadata.is_cdf_enabled(
        import_table_path
    ), """
    Versioning not enable . To start recording change data, use
    `ALTER TABLE table_name SET TBLPROPERTIES (delta.enableChangeDataFeed=true)`.
    """

    assert from_version, "Read from version number not provided"

    # If to version is not provided , read until the last version.
    if not to_version:

        collected_history = metadata.get_collected_history(import_table_path)
        to_version = metadata.get_current_version(collected_history)

    # Read the ingested data between the from_version until the to_version
    df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version)
        .option("endingVersion", to_version)
        .table(import_table_path)
        .where("_change_type = 'insert'")
    ).drop("_change_type", "_commit_version", "_commit_timestamp")

    assert df is not None

    return df


def import_dataframe_incrementally(
    import_table_name: str = "",
    import_table_catalog_path: str = "",
    metadata_checkpoint_path: str = "",
) -> DataFrame:
    # Create or receive spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    import_table_path = import_table_catalog_path + import_table_name

    # Assertions:
    if not spark.catalog.tableExists(import_table_path):
        raise ValueError(f"Table {import_table_path} does not exist.")

    # Assertion if the enableChangeDataFeed was not enabled
    assert metadata.is_cdf_enabled(
        import_table_path
    ), """
    Versioning not enable . To start recording change data, use
    `ALTER TABLE table_name SET TBLPROPERTIES (delta.enableChangeDataFeed=true)`.
    """

    # Get the files at location , create the folder if it does not exist
    metadata_versions = metadata.get_metadata_versions(metadata_checkpoint_path)

    # Collect the latest history of the input table
    collected_history = metadata.get_collected_history(import_table_path)

    # Find the version that enabels ChangeDataFeed
    start_version = metadata.get_first_relevant_version(collected_history)

    # Future proof (history only lasts for 30 days) , if the table has cdf enabled and its not in history ,
    # take the last version to indicate the start .
    if not start_version:
        start_version = collected_history[-1].version
    else:
        start_version = start_version[-1]

    # Read out the table
    # If this is the first version of the table.
    # CDF can not read from the first version if it was not created with the enableChangeDataFeed as a paramiter
    if not metadata_versions:
        df = spark.read.table(import_table_path)

        # Create the first version of the metadata file
        new_metadata_path = metadata.create_new_metadata_file_path(
            metadata_checkpoint_path, None
        )

    # If there are prevous versions
    else:
        # Get previous version and get its value
        last_metadata_version = metadata.get_metadata_versions(
            metadata_checkpoint_path, last_version_only=True
        )
        last_metadata_df = metadata.read_metadata(last_metadata_version.path)
        last_processed_version = int(
            last_metadata_df.select("readVersion").collect()[0][0]
        )

        # Get current version
        current_version = metadata.get_current_version(collected_history)

        new_metadata_path = metadata.create_new_metadata_file_path(
            metadata_checkpoint_path, last_metadata_version
        )

        # If the latest version is a version already processed
        # Or the version that just set the properties of the table to change data feed enable, returns empty dataframe
        if (
            current_version == last_processed_version
            or current_version == start_version
        ):
            df = spark.createDataFrame([], spark.read.table(import_table_path).schema)

        # If new vesion avalable , read new data that was inserted
        else:
            df = import_dataframe_from_version(
                import_table_name,
                import_table_catalog_path,
                from_version=last_processed_version + 1,
                to_version=current_version,
            )

    # Process statistics and metadata of this batch
    current_metadata = metadata.create_current_metadata(df, collected_history)
    # Save new version of metadata
    metadata.save_metadata(current_metadata, new_metadata_path)

    # Check that a dataframe could be created
    assert df is not None

    return df

# COMMAND ----------

metadata_path = "wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/metadata/gold/_metadata/"
# metadata_versions = metadata.get_metadata_versions(metadata_path)
# metadata.del_old_versions(metadata_versions)
batch_df = import_dataframe_incrementally("production_silver", "ser6db." , metadata_path)

if batch_df.count() == 0:  # If no files to be processed --> no operation.
    dbutils.notebook.exit("No files to be processed --> stopped execution")

# COMMAND ----------

client = MlflowClient()  
      
runs = client.search_runs(  
experiment_ids=[3566446926298132],  
filter_string=f"tags.mlflow.runName = 'Current_models'",  
order_by=["start_time DESC"],  
max_results=1  
)  

start_end_excercise_id_model = mlflow.spark.load_model(f"runs:/{runs[0].info.run_id}/start-end-excercise-id-model")  
concentric_model = mlflow.spark.load_model(f"runs:/{runs[0].info.run_id}/concentric-model")  
eccentric_model = mlflow.spark.load_model(f"runs:/{runs[0].info.run_id}/eccentric-model")  

# COMMAND ----------

start_end_excercise_id_predictions = (
    start_end_excercise_id_model.transform(batch_df)
    .select(["filename", "measure_date", "timestamp", "time_vector", "prediction"])
    .withColumnRenamed("prediction", "start_end_excercise_id")
)
batch_df = batch_df.join(
    start_end_excercise_id_predictions,
    on=["filename", "measure_date", "timestamp", "time_vector"],
    how="left",
)
concentric_model_predictions = (
    concentric_model.transform(batch_df)
    .select(["filename", "measure_date", "timestamp", "time_vector", "prediction"])
    .withColumnRenamed("prediction", "concentric")
)
eccentric_model_predictions = (
    eccentric_model.transform(batch_df)
    .select(["filename", "measure_date", "timestamp", "time_vector", "prediction"])
    .withColumnRenamed("prediction", "eccentric")
)

window_spec = Window.partitionBy("filename").orderBy("time_vector")

batch_df = (
    batch_df.withColumn(
        "start_end_excercise_id",
        F.max("start_end_excercise_id").over(window_spec.rowsBetween(-50, 50)),
    )
    .withColumn(
        "start_end_excercise_id",
        F.when(F.col("start_end_excercise_id") == 7, "Squat")
        .when(F.col("start_end_excercise_id") == 5, "Deadlift")
        .otherwise("Unknown"),
    )
    .join(
        concentric_model_predictions,
        on=["filename", "measure_date", "timestamp", "time_vector"],
        how="left",
    )
    .join(
        eccentric_model_predictions,
        on=["filename", "measure_date", "timestamp", "time_vector"],
        how="left",
    )
)

groups_window = Window.partitionBy("filename", "events").orderBy("time_vector")
events_groups_window = Window.partitionBy("group_id").orderBy("events_groups")

batch_df = batch_df.withColumn(
    "events",
    F.when(
        (F.col("start_end_excercise_id") != "Unknown")
        & (
            (
                (
                    (F.col("concentric").cast("boolean")).cast("int").bitwiseXOR(F.col("eccentric").cast("int"))
                )
            )
            == 1
        ),
        1,
    ).otherwise(0),
).withColumn(
    "events_groups",
    F.row_number().over(window_spec) - F.row_number().over(groups_window),
)

batch_df = (
    batch_df.withColumn(
        "rest_time",
        F.when(
            (F.col("eccentric") == 0) | (F.col("concentric") == 0), F.col("time_vector")
        ).otherwise(0),
    )
    .withColumn(
        "concentric_time", F.when((F.col("concentric") == 1), F.col("time_vector"))
    )
    .withColumn(
        "eccentric_time", F.when((F.col("eccentric") == 1), F.col("time_vector"))
    )
)

# COMMAND ----------

batch_grouped_df = batch_df.groupBy("events_groups", "start_end_excercise_id","measure_date").agg(
    (F.max("concentric_time") - F.min("concentric_time")).alias("concentric_time"),
    (F.max("eccentric_time") - F.min("eccentric_time")).alias("eccentric_time"),
    (F.max("rest_time") - F.min("rest_time")).alias("rest_time")
).withColumn(  
    "group_id",  
    F.when(F.col("start_end_excercise_id") != "Unknown", 1).otherwise(0)
).withColumn(  
    "group_id",F.row_number().over(Window.orderBy("events_groups")) - F.row_number().over(events_groups_window))

overall_stats = batch_grouped_df.groupBy("measure_date").agg(
    F.countDistinct(F.when(F.col('start_end_excercise_id') != "Unknown", F.col('group_id'))).alias("unique_excercises"),
    F.mode(F.when(F.col('start_end_excercise_id')!= "Unknown",F.col('start_end_excercise_id'))).alias('excercise'),
    F.sum("concentric_time").alias("total_concentric_time"),
    F.sum("eccentric_time").alias("total_eccentric_time"),
    F.count(F.when(F.col('concentric_time') > 0, 1)).alias("count_concentric_times"),
    F.count(F.when(F.col('eccentric_time') > 0, 1)).alias("count_eccentric_times"),
    F.sum("rest_time").alias("total_rest_time"),
    F.avg("concentric_time").alias("avg_concentric_time"),
    F.avg("eccentric_time").alias("avg_eccentric_time"),
    F.avg("rest_time").alias("avg_rest_time")
).collect()

overall_stats_json = {    
    "measure_dates": []  
}


for row in overall_stats:  
    stats_entry = {  
        "excercise":row['excercise'] if row['excercise'] else "Unknown",
        "measure_date": str(row['measure_date']),  
        "sets": row['unique_excercises'],  
        "reps" : (row['count_eccentric_times'] + row['count_concentric_times'])/2,
        "total_concentric_time": row['total_concentric_time'],  
        "total_eccentric_time": row['total_eccentric_time'],  
        "count_concentric_times": row['count_concentric_times'],  
        "count_eccentric_times": row['count_eccentric_times'],  
        "total_rest_time": row['total_rest_time'],  
        "avg_concentric_time": row['avg_concentric_time'],  
        "avg_eccentric_time": row['avg_eccentric_time'],  
        "avg_rest_time": row['avg_rest_time'] 
    }  
    overall_stats_json["measure_dates"].append(stats_entry)  
  
stats_json = json.dumps(overall_stats_json, indent=4)  
  
print(stats_json)

# COMMAND ----------


data = batch_df.toPandas()  

def plotlyPlot(data , prefix , offset):
    fig = go.Figure()  
    
    fig.add_trace(go.Scatter3d(  
        x=data[f'{prefix}X_cs'],  
        y=data[f'{prefix}Y_cs'],  
        z=data[f'{prefix}Z_cs'],  
        mode='markers',  
        marker=dict(  
            size=1,  
            color=data['events_groups'],
            colorscale='Turbo',
            opacity=0.8 
        ),  
        name=f'{prefix}XYZ for chest strap sensor'  
    ))  

    fig.add_trace(go.Scatter3d(  
        x=data[f'{prefix}X_ws'] + offset[0],  
        y=data[f'{prefix}Y_ws'] + offset[1],  
        z=data[f'{prefix}Z_ws'] + offset[2],  
        mode='markers',  
        marker=dict(  
            size=1,  
            color=data['events_groups'],  
            colorscale='Viridis', 
            opacity=0.8  
        ),  
        name=f'{prefix}XYZ for wrist sensor'  
    ))  

    fig.update_layout(  
        scene=dict(  
            xaxis_title=f'{prefix}X',  
            yaxis_title=f'{prefix}Y',  
            zaxis_title=f'{prefix}Z'  
        ),  
        title='3D Scatter Plot of Inertial Data'  
    )  
    return fig
    
plotlyPlot(data ,'accel',(500,0,0) ).show()  


# COMMAND ----------

(
    batch_df.write.mode('append')
    .option("maxRecordsPerFile", 1000000)
    .format("delta")
    .saveAsTable('ser6db.production_gold')
)

# COMMAND ----------

def send_email_with_results(smtp_server, port, sender_email, sender_password, recipient_email, subject, body, html_contents):  

    msg = MIMEMultipart()  
    msg['From'] = sender_email  
    msg['To'] = recipient_email  
    msg['Subject'] = subject  
  
    msg.attach(MIMEText(body, 'plain'))  
  
    for html_content, filename in html_contents:  
        part = MIMEText(html_content, 'html')  
        part.add_header(  
            "Content-Disposition",  
            f"attachment; filename={filename}",  
        )  
        msg.attach(part)  
      
    try:  
        server = smtplib.SMTP(smtp_server, port)  
        server.starttls()  
        server.login(sender_email, sender_password)  
            
        server.sendmail(sender_email, recipient_email, msg.as_string())  
        print("Email sent successfully!")  
    except Exception as e:  
        print(f"Error: {e}")  
    finally:  
        server.quit()   
  
smtp_server = "smtp.gmail.com"  # Gmail SMTP server  
port = 587  # For starttls  
sender_email = "serthises@gmail.com"  
sender_password = "kbsdrjuhietldrei"  
recipient_email = "rares13rares13@gmail.com"  
subject = "Excercise Summary"  

html_contents = [  
    (plotlyPlot(data ,'quat',(500,0,0)).to_html(full_html=False ) , "Quaternion.html"),
    (plotlyPlot(data ,'accel',(500,0,0)).to_html(full_html=False ) , "Accelerometer.html"),
    (plotlyPlot(data ,'gyro',(500,0,0)).to_html(full_html=False ) , "Gyroscope.html"),
]  

if overall_stats:
    send_email_with_results(smtp_server, port, sender_email, sender_password, recipient_email, subject, stats_json, html_contents)  

