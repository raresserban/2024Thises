# Databricks notebook source
from pyspark.sql import DataFrame , Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window ,WindowSpec
from pyspark.sql.types import *
from typing import Union
import numpy as np

accountkey = dbutils.secrets.get(scope = 'landingzone' , key = 'container' )
spark.conf.set("fs.azure.account.key.landingzone6291.blob.core.windows.net" , accountkey)

# COMMAND ----------

# MAGIC %md
# MAGIC #Helper Funcitons

# COMMAND ----------

def rising_edge(
    df: DataFrame,
    window: WindowSpec,
    input_signal: Union[Column, str],
    output_signal: str,
) -> DataFrame:
   
    if isinstance(input_signal, str):
        input_signal = F.col(input_signal).cast('int')

    temp_col_name = "temp_input_signal"
    temp_lag_col_name = "temp_lag_input_signal"

    # Cast the input to a boolean type
    df = df.withColumn(temp_col_name, input_signal.cast("boolean"))
    temp_input_signal = F.col(temp_col_name)

    # Create shifted column of the input signal in a temporary signal.
    df = df.withColumn(
        temp_lag_col_name,
        F.lag(temp_col_name, offset=1, default=None).over(window),
    )
    temp_lag_input_signal = F.col(temp_lag_col_name)

    # Create the output signal based on the evaluation of the conditions and cast to integer
    df = df.withColumn(
        output_signal,
        F.when((temp_input_signal & ~temp_lag_input_signal), 1).otherwise(0),
    )
    df = df.withColumn(output_signal, F.col(output_signal).cast("int"))

    # Drop the temporary Signal
    df = df.drop(temp_input_signal)
    df = df.drop(temp_lag_input_signal)

    return df


def falling_edge(
    df: DataFrame,
    window: WindowSpec,
    input_signal: Union[Column, str],
    output_signal: str,
) -> DataFrame:

    if isinstance(input_signal, str):
        input_signal = F.col(input_signal).cast('int')

    temp_col_name = "temp_input_signal"
    temp_lag_col_name = "temp_lag_input_signal"

    # Cast the input to a boolean type
    df = df.withColumn(temp_col_name, input_signal.cast("boolean"))
    temp_input_signal = F.col(temp_col_name)

    # Create shifted column of the input signal in a temporary signal.
    df = df.withColumn(
        temp_lag_col_name,
        F.lag(temp_col_name, offset=1, default=None).over(window),
    )
    temp_lag_input_signal = F.col(temp_lag_col_name)

    # Create the output signal based on the evaluation of the conditions and cast to integer
    df = df.withColumn(
        output_signal,
        F.when((~temp_input_signal & temp_lag_input_signal), 1).otherwise(0),
    )
    df = df.withColumn(output_signal, F.col(output_signal).cast("int"))

    # Drop the temporary Signal
    df = df.drop(temp_input_signal)
    df = df.drop(temp_lag_input_signal)

    return df

 
def extract_date_from_filename(column: F.col):

    # Format: yyyyMMdd-HHmmss --> 20230717-122130  
    REGEX_YYYYMMDD_HHMMSS_SSSSSS = r"(\d{8}[-]\d{6}_\d{6})"  


    # Create regex functions to find the first match in the column and extract it
    func_regex_yyyyMMdd_HHmmss_SSSSSS = F.regexp_extract(column, REGEX_YYYYMMDD_HHMMSS_SSSSSS, 1)  

    # Cast to timestamp depending on format
    return (  
        F.when(  
            func_regex_yyyyMMdd_HHmmss_SSSSSS != "",  
            F.to_timestamp(func_regex_yyyyMMdd_HHmmss_SSSSSS, "yyyyMMdd-HHmmss_SSSSSS"),  
        )  
        .otherwise(None)  
    ) 

# COMMAND ----------

class UpsertData:
    def __init__(
        self,
        importhiveDirectoryName: str,
        exporthiveDirectoryName: str,
        tableName: str,
        selectDict: dict = None,
    ):
        self.importhiveDirectoryName = (
            importhiveDirectoryName
            if importhiveDirectoryName.endswith(".")
            else importhiveDirectoryName + "."
        )
        self.exporthiveDirectoryName = (
            exporthiveDirectoryName
            if exporthiveDirectoryName.endswith(".")
            else exporthiveDirectoryName + "."
        )
        self.tableName = tableName
        self.selectDict = selectDict  # {old : new}

    def _getTypeAsString(self, data_type):
        """
        Converts a data type to its string representation.

        Args:
            data_type: The data type to convert.

        Returns:
            A string representation of the data type.
        """
        if isinstance(
            data_type,
            (
                StringType,
                IntegerType,
                BooleanType,
                DoubleType,
                FloatType,
                ShortType,
                DateType,
                TimestampType,
            ),
        ):
            return data_type.typeName().upper()
        elif isinstance(data_type, LongType):
            return "BIGINT"
        elif isinstance(data_type, DecimalType):
            return f"DECIMAL({data_type.precision},{data_type.scale})"
        elif isinstance(data_type, MapType):
            return f"MAP<{self._getTypeAsString(data_type.keyType)}, {self._getTypeAsString(data_type.valueType)}>"
        elif isinstance(data_type, ArrayType):
            return f"ARRAY<{self._getTypeAsString(data_type.elementType)}>"
        elif isinstance(data_type, StructType):
            return f"STRUCT<{', '.join([f'{field.name}: {self._getTypeAsString(field.dataType)}' for field in data_type.fields])}>"
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")

    def _readStreamAt10ms(self, softMaxBytesPerBatch: int):
        """
        Reads a stream of data from the import directory with optional selection and renaming of columns.

        Args:
            softMaxBytesPerBatch (int): The soft maximum number of bytes per batch.
        Returns:
            A DataFrame with the streamed data.
        """
        if self.selectDict:
            return (
                spark.readStream.format("delta")
                .option("readChangeFeed", "true")
                .option("maxBytesPerTrigger", softMaxBytesPerBatch)
                .table(f"{self.importhiveDirectoryName}{self.tableName}")
                .select(list(self.selectDict.keys()))
                .withColumnsRenamed(self.selectDict)
            )
        else:
            return (
                spark.readStream.format("delta")
                .option("readChangeFeed", "true")
                .option("maxBytesPerTrigger", softMaxBytesPerBatch)
                .table(f"{self.importhiveDirectoryName}{self.tableName}")
            )

    def _getTableSchema(self , tableName):
        """
        Retrieves the schema of the master table or the schema table, with optional selection and renaming of columns.

        Returns:
            The schema of the table.
        """
        if spark.catalog.tableExists(
            f"{self.exporthiveDirectoryName}{tableName}"
        ):
            return spark.read.table(
                f"{self.exporthiveDirectoryName}{tableName}"
            ).schema
        elif self.selectDict:

            assert all(
                key
                in spark.read.table(
                    f"{self.importhiveDirectoryName}{self.tableName}"
                ).schema.names
                for key in self.selectDict.keys()
            ), "Not all keys in tables schema"

            return (
                spark.read.table(f"{self.importhiveDirectoryName}{self.tableName}")
                .select(list(self.selectDict.keys()))
                .withColumnsRenamed(self.selectDict)
                .schema
            )
        else:
            return spark.read.table(
                f"{self.importhiveDirectoryName}{self.tableName}"
            ).schema

    def _transformStructureType(self, struct_type):
        """
        Transforms a StructType into a SQL string representation.

        Args:
            struct_type: The StructType to transform.

        Returns:
            A SQL string representation of the StructType.
        """
        transformed_fields = []
        for field in struct_type.fields:
            transformed_fields.append(
                f"{field.name} {self._getTypeAsString(field.dataType)}"
            )
        return ", ".join(transformed_fields)

    def createMasterTable(self,tableName,schema, partitionOn: list = None):
        """
        Creates the master table if it does not exist, using the Delta format.

        Args:
            masterTablePath (str): The file system path where the master table is located.
            partitionOn (list, optional): The columns to partition the table on. Defaults to None.
        """

        if not spark.catalog.tableExists(
            f"{self.exporthiveDirectoryName}{tableName}"
        ):
            CREATE_TABLE = f"""
                        CREATE TABLE {self.exporthiveDirectoryName}{tableName}
                        ({self._transformStructureType(StructType(sorted(schema, key=lambda x:  ({'string': 0, 'timestamp': 1, 'double': 2, 'float': 3}.get(x.dataType.typeName(), 4), x.name))))})
                        USING DELTA
                        TBLPROPERTIES ( 
                        delta.autoOptimize.autoCompact = 'true',
                        delta.autoOptimize.optimizeWrite = 'true' ,
                        delta.enableChangeDataFeed = 'true' 
                        )"""
            PARTITIONED_BY = (
                "PARTITIONED BY (" + ", ".join(partitionOn) + ")" if partitionOn else ""
            )
            spark.sql(
                f"""
                        {CREATE_TABLE}
                        {PARTITIONED_BY}"""
            )
        return self

    def mergeStreamWithMasterTable(
        self,
        UpsertClass,
        checkpointDirectoryPath: str,
        softMaxBytesPerBatch: int = 10 * 1024 * 1024 * 1024,
    ):
        """
        Merges a streamed DataFrame with the master table, using a specified upsert function.

        Args:
            UpsertClass (function): The function to use for merging (upserting) the data.
            softMaxBytesPerBatch (int, optional): The soft maximum number of bytes per batch. Defaults to 10GB.
        """

        # Read the streaming DataFrame

        streamingDF = self._readStreamAt10ms(softMaxBytesPerBatch)

        print("Running Query ...")
        # Proceed with the joined DataFrame
        query = (
            streamingDF.writeStream.foreachBatch(UpsertClass)
            .outputMode("append")
            .option(
                "checkpointLocation",
                f"{checkpointDirectoryPath}",
            )
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    def loadMainTable(self , tableName):
        """
        Loads the main table as a DataFrame.

        Returns:
            DataFrame: The loaded master table.
        """
        return spark.table(f"{self.exporthiveDirectoryName}{tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resampling and Downsampling
# MAGIC
# MAGIC As the signals are recorded with a 100ms raster, but interpolated in the master table with 10ms, a resampling and downsampling is necessary
# MAGIC
# MAGIC time vector = 1234.123
# MAGIC
# MAGIC time_sec = 1234
# MAGIC
# MAGIC time_100ms = 1234.1
# MAGIC
# MAGIC time_10ms = 1234.12
# MAGIC
# MAGIC dec_100ms = 0.1
# MAGIC
# MAGIC dec_10ms = 0.02

# COMMAND ----------

# Resample data  by evaluating decimals
# RESAMPLING_COND = (F.col("dec_10ms") % 0.02) == 0  # 20ms raster
# RESAMPLING_COND = F.col("dec_10ms") == 0  # 100ms raster
# RESAMPLING_COND = (F.col(dec_100ms) == 0) | (F.col(dec_100ms) == 5) # 500ms raster

# COMMAND ----------

class Upsert:
    def __init__(
        self,
        exporthiveDirectoryName,
        resamplingCond=None,
    ):

        self.exporthiveDirectoryName = exporthiveDirectoryName
        self.resamplingCond = resamplingCond

    def updateDeltaTable(self, microBatchDF, batch):

        # Perform transformation on each batch
        # Create Signal to allow resampling on time_vector signal (removal of decimals on the time vector signal)

        microBatchDF = (
            microBatchDF.withColumn("time_sec", F.floor(microBatchDF["time_vector"]))
            .withColumn("time_100ms", F.floor(microBatchDF["time_vector"] * 10) / 10)
            .withColumn("time_10ms", F.floor(microBatchDF["time_vector"] * 100) / 100)
            .withColumn("dec_100ms", F.round(F.expr("time_100ms - time_sec") * 10) / 10)
            .withColumn(
                "dec_10ms", F.round(F.expr("time_10ms - time_100ms") * 100) / 100
            )
        )

        if self.resamplingCond is not None:
            microBatchDF = microBatchDF.filter(
                self.resamplingCond
            )  # Filter the data (Query data based on defined conditions)

        microBatchDF = microBatchDF.drop(
            "time_sec", "time_100ms", "time_10ms", "dec_100ms", "dec_10ms"
        )
        microBatchDF = microBatchDF.replace([np.inf, -np.inf], np.NaN)
        exprs = [
            F.when(F.col(col) == 0.0, None).otherwise(F.col(col)).alias(col)
            if col
            in [
                "accelX_ws",
                "accelY_ws",
                "accelZ_ws",
                "gyroX_ws",
                "gyroY_ws",
                "gyroZ_ws",
                "accelX_cs",
                "accelY_cs",
                "accelZ_cs",
                "gyroX_cs",
                "gyroY_cs",
                "gyroZ_cs",
            ]
            else F.col(col)
            for col in microBatchDF.columns
        ]
        microBatchDF = microBatchDF.select(exprs)
        microBatchDF = microBatchDF.dropna()

        microBatchDF = microBatchDF.filter(F.col("filename").contains("_MPU6500_"))

        window_spec = Window.partitionBy("filename").orderBy("time_vector")
        unbounded_spec = (
            Window.partitionBy("filename")
            .orderBy("time_vector")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        events_window = Window.partitionBy("filename", "all_events").orderBy(
            "time_vector"
        )

        start_end_excercise_window = Window.partitionBy(
            "filename", "start_end_excercise"
        ).orderBy("time_vector")

        groups_start_end_excercise_window = Window.partitionBy(
            "filename", "group_start_end_excercise_events"
        ).orderBy("time_vector")

        peak_trough_events_window = Window.partitionBy(
            "filename", "events"
        ).orderBy("time_vector")

        peak_trough_unbounded_groups_window = (
            Window.partitionBy("filename", "events_groups")
            .orderBy("time_vector")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        # ///////////////////////////////
        microBatchDF = microBatchDF.withColumn(
            "filename", F.regexp_extract(F.col("filename"), r"[^/]+$", 0)
        )

        microBatchDF = microBatchDF.withColumn(
            "measure_date", extract_date_from_filename(F.col("filename"))
        )

        microBatchDF = microBatchDF.withColumn(
            "timestamp",
            F.from_unixtime(
                F.unix_timestamp(
                    F.lit(F.col("measure_date")), "yyyy-MM-dd HH:mm:ss.SSSSSS"
                )
                + (F.col("time_vector")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

        microBatchDF = microBatchDF.withColumn(
            "mpuVersion", F.regexp_extract(F.col("filename"), r"(MPU\d{4})", 1)
        )
        microBatchDF = (
            microBatchDF.withColumn("trough", F.col("trough").cast("int"))
            .withColumn("peak", F.col("peak").cast("int"))
            .withColumn("start_end_excercise", F.col("start_end_excercise").cast("int"))
        )

        microBatchDF = microBatchDF.withColumn(
            "all_events",
            (
                F.col("trough").cast("boolean")
                | F.col("peak").cast("boolean")
                | F.col("start_end_excercise").cast("boolean")
            ).cast("int"),
        )

        microBatchDF = microBatchDF.withColumn(
            "group_events",
            F.row_number().over(window_spec) - F.row_number().over(events_window),
        )
        microBatchDF = microBatchDF.withColumn(
            "first_group_id",
            F.when(
                (F.row_number().over(events_window) == 1) & (F.col("all_events") == 1),
                F.col("group_events"),
            ).otherwise(F.lit(0)),
        )
        microBatchDF = microBatchDF.withColumn(
            "first_group_id", F.max("first_group_id").over(unbounded_spec)
        )
        microBatchDF = microBatchDF.withColumn(
            "first_event",
            F.when(
                F.col("group_events") == F.col("first_group_id"), F.lit(1)
            ).otherwise(F.lit(0)),
        )
        microBatchDF = microBatchDF.withColumn(
            "excercise_id",
            F.when(
                F.col("first_event") == 1,
                (F.col("start_end_excercise") * 4)
                + (F.col("peak") * 2)
                + F.col("trough"),
            ).otherwise(None),
        ).withColumn("excercise_id", F.max("excercise_id").over(unbounded_spec))

        microBatchDF = (
            microBatchDF.withColumn(
                "trough",
                F.when(F.col("first_event") == 1, F.lit(0)).otherwise(F.col("trough")),
            )
            .withColumn(
                "peak",
                F.when(F.col("first_event") == 1, F.lit(0)).otherwise(F.col("peak")),
            )
            .withColumn(
                "start_end_excercise",
                F.when(F.col("first_event") == 1, F.lit(0)).otherwise(
                    F.col("start_end_excercise")
                ),
            )
        )

        microBatchDF = microBatchDF.drop(
            "all_events", "group_events", "first_group_id", "first_event"
        )

        microBatchDF = falling_edge(
            microBatchDF,
            window_spec,
            "start_end_excercise",
            "start_end_excercise_falling",
        )
        microBatchDF = rising_edge(
            microBatchDF,
            window_spec,
            "start_end_excercise",
            "start_end_excercise_rising",
        )

        microBatchDF = microBatchDF.withColumn(
            "cumu_start_end_excercise_rising",
            F.sum("start_end_excercise_rising").over(window_spec),
        ).withColumn(
            "cumu_start_end_excercise_falling",
            F.sum("start_end_excercise_falling").over(window_spec),
        )
        microBatchDF = microBatchDF.withColumn(
            "start_end_excercise",
            (
                (F.col("cumu_start_end_excercise_falling") % 2 != 0)
                | (F.col("cumu_start_end_excercise_rising") % 2 != 0)
            ).cast("int"),
        )
        microBatchDF = microBatchDF.withColumn(
            "start_end_excercise_id",
            F.when(F.col("start_end_excercise") == 1, F.col("excercise_id")).otherwise(
                F.lit(0)
            ),
        )

        microBatchDF = microBatchDF.withColumn(
            "group_start_end_excercise_events",
            F.row_number().over(window_spec)
            - F.row_number().over(start_end_excercise_window),
        )
        microBatchDF = microBatchDF.withColumn(
            "start_event_time_vector",
            F.first("time_vector").over(groups_start_end_excercise_window),
        )
        microBatchDF = microBatchDF.withColumn(
            "relative_time", F.col("time_vector") - F.col("start_event_time_vector")
        )

        microBatchDF = microBatchDF.drop(
            "cumu_start_end_excercise_falling",
            "cumu_start_end_excercise_rising",
            "start_end_excercise_falling",
            "start_end_excercise_rising",
            "start_end_excercis",
            "group_start_end_excercise_events",
            "start_event_time_vector",
        )
        
        microBatchDF = microBatchDF.withColumn(
            "events",
            F.when(
                (F.col("start_end_excercise_id") == 0)
                | (
                    (
                        (
                            F.col("peak").cast("boolean")
                            | F.col("trough").cast("boolean")
                        ).cast("int")
                    )
                    == 1
                ),
                0,
            ).otherwise(1),
        )

        microBatchDF = microBatchDF.withColumn(
            "events_groups",
            F.row_number().over(window_spec)
            - F.row_number().over(peak_trough_events_window),
        )

        microBatchDF = microBatchDF.withColumn(
            "eccentric",
            F.when(
                ((F.col("events") == 1) & (F.lag(F.col("peak")).over(window_spec) == 1))
                | (
                    (F.col("events") == 1)
                    & (F.lead(F.col("trough")).over(window_spec) == 1)
                ),
                1,
            ).otherwise(0),
        )

        microBatchDF = microBatchDF.withColumn(
            "concentric",
            F.when(
                (
                    (F.col("events") == 1)
                    & (F.lag(F.col("trough")).over(window_spec) == 1)
                )
                | (
                    (F.col("events") == 1)
                    & (F.lead(F.col("peak")).over(window_spec) == 1)
                ),
                1,
            ).otherwise(0),
        )

        microBatchDF = microBatchDF.withColumn(
            "eccentric", F.max("eccentric").over(peak_trough_unbounded_groups_window)
        )

        microBatchDF = microBatchDF.withColumn(
            "concentric", F.max("concentric").over(peak_trough_unbounded_groups_window)
        )

        microBatchDF = microBatchDF.drop("events", "events_groups")


        microBatchDFTraining = microBatchDF.filter(F.col("excercise_id") != 0).select(
            [col for col in microBatchDF.columns if col not in ["_rescued_data"]]
        )

        microBatchDFProduction = microBatchDF.filter(F.col("excercise_id") == 0).select(
            [
                col
                for col in microBatchDFTraining.columns
                if col
                not in [
                    "_rescued_data",
                    "excercise_id",
                    "relative_time",
                    "start_end_excercise",
                    "start_end_excercise_id",
                    "eccentric",
                    "concentric",
                ]
            ]
        )

        # ///////////////////////////////
        microBatchDFTraining = microBatchDFTraining.repartition("filename")
        microBatchDFProduction = microBatchDFProduction.repartition("filename")
        # End of transfomationx

        training_merge_in_on_condition = " AND ".join(
            [
                f"a.{col} = b.{col}"
                for col in microBatchDFTraining.columns
                if "time" in col or col == "filename"
            ]
        )
        sqlQueryToTraining = f"""
                MERGE INTO {self.exporthiveDirectoryName}training_silver a
                USING update_training b
                ON {training_merge_in_on_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """

        production_merge_in_on_condition = " AND ".join(
            [
                f"c.{col} = d.{col}"
                for col in microBatchDFProduction.columns
                if "time" in col or col == "filename"
            ]
        )
        sqlQueryToProduction = f"""
                MERGE INTO {self.exporthiveDirectoryName}production_silver c
                USING update_production d
                ON {production_merge_in_on_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """

        microBatchDFTraining.createOrReplaceTempView("update_training")
        microBatchDFTraining.sparkSession.sql(sqlQueryToTraining)

        microBatchDFProduction.createOrReplaceTempView("update_production")
        microBatchDFProduction.sparkSession.sql(sqlQueryToProduction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Master table 

# COMMAND ----------

# {old:new}
master_signal_dict = {
    "timevector": "time_vector",
    "ax1": "accelX_ws",  # Accelerometer X-axis for wrist sensor
    "ay1": "accelY_ws",  # Accelerometer Y-axis for wrist sensor
    "az1": "accelZ_ws",  # Accelerometer Z-axis for wrist sensor
    "gx1": "gyroX_ws",  # Gyroscope X-axis for wrist sensor
    "gy1": "gyroY_ws",  # Gyroscope Y-axis for wrist sensor
    "gz1": "gyroZ_ws",  # Gyroscope Z-axis for wrist sensor
    "qx1": "quatX_ws",  # Quaternion X component for wrist sensor
    "qy1": "quatY_ws",  # Quaternion Y component for wrist sensor
    "qz1": "quatZ_ws",  # Quaternion Z component for wrist sensor
    "qw1": "quatW_ws",  # Quaternion W component for wrist sensor
    "wax1": "worldAccelX_ws",  # Worldframe Accelerometer X-axis for wrist sensor
    "way1": "worldAccelY_ws",  # Worldframe Accelerometer Y-axis for wrist sensor
    "waz1": "worldAccelZ_ws",  # Worldframe Accelerometer Z-axis for wrist sensor
    "wgx1": "worldGyroX_ws",  # Worldframe Gyroscope X-axis for wrist sensor
    "wgy1": "worldGyroY_ws",  # Worldframe Gyroscope Y-axis for wrist sensor
    "wgz1": "worldGyroZ_ws",  # Worldframe Gyroscope Z-axis for wrist sensor
    "temp1": "temperature_ws",  # Temperature for wrist sensor
    "ax2": "accelX_cs",  # Accelerometer X-axis for chest strap sensor
    "ay2": "accelY_cs",  # Accelerometer Y-axis for chest strap sensor
    "az2": "accelZ_cs",  # Accelerometer Z-axis for chest strap sensor
    "gx2": "gyroX_cs",  # Gyroscope X-axis for chest strap sensor
    "gy2": "gyroY_cs",  # Gyroscope Y-axis for chest strap sensor
    "gz2": "gyroZ_cs",  # Gyroscope Z-axis for chest strap sensor
    "qx2": "quatX_cs",  # Quaternion X component for chest strap sensor
    "qy2": "quatY_cs",  # Quaternion Y component for chest strap sensor
    "qz2": "quatZ_cs",  # Quaternion Z component for chest strap sensor
    "qw2": "quatW_cs",  # Quaternion W component for chest strap sensor
    "wax2": "worldAccelX_cs",  # Worldframe Accelerometer X-axis for chest strap sensor
    "way2": "worldAccelY_cs",  # Worldframe Accelerometer Y-axis for chest strap sensor
    "waz2": "worldAccelZ_cs",  # Worldframe Accelerometer Z-axis for chest strap sensor
    "wgx2": "worldGyroX_cs",  # Worldframe Gyroscope X-axis for chest strap sensor
    "wgy2": "worldGyroY_cs",  # Worldframe Gyroscope Y-axis for chest strap sensor
    "wgz2": "worldGyroZ_cs",  # Worldframe Gyroscope Z-axis for chest strap sensor
    "temp2": "temperature_cs",  # Temperature for chest strap sensor
    "button1": "trough",  # Classifying trough of the excercise
    "button2": "peak",  # Classifying peak of the excercise
    "button3": "start_end_excercise",  # Classifying For identifing the start and end of the excercise
    "filename": "filename",  # File name
}
trainingSchema = StructType(
    [
        StructField("time_vector", DoubleType(), False),
        StructField("relative_time", DoubleType(), False),
        StructField("accelX_ws", DoubleType(), False),
        StructField("accelY_ws", DoubleType(), False),
        StructField("accelZ_ws", DoubleType(), False),
        StructField("gyroX_ws", DoubleType(), False),
        StructField("gyroY_ws", DoubleType(), False),
        StructField("gyroZ_ws", DoubleType(), False),
        StructField("quatX_ws", DoubleType(), False),
        StructField("quatY_ws", DoubleType(), False),
        StructField("quatZ_ws", DoubleType(), False),
        StructField("quatW_ws", DoubleType(), False),
        StructField("worldAccelX_ws", DoubleType(), False),
        StructField("worldAccelY_ws", DoubleType(), False),
        StructField("worldAccelZ_ws", DoubleType(), False),
        StructField("worldGyroX_ws", DoubleType(), False),
        StructField("worldGyroY_ws", DoubleType(), False),
        StructField("worldGyroZ_ws", DoubleType(), False),
        StructField("temperature_ws", DoubleType(), False),
        StructField("accelX_cs", DoubleType(), False),
        StructField("accelY_cs", DoubleType(), False),
        StructField("accelZ_cs", DoubleType(), False),
        StructField("gyroX_cs", DoubleType(), False),
        StructField("gyroY_cs", DoubleType(), False),
        StructField("gyroZ_cs", DoubleType(), False),
        StructField("quatX_cs", DoubleType(), False),
        StructField("quatY_cs", DoubleType(), False),
        StructField("quatZ_cs", DoubleType(), False),
        StructField("quatW_cs", DoubleType(), False),
        StructField("worldAccelX_cs", DoubleType(), False),
        StructField("worldAccelY_cs", DoubleType(), False),
        StructField("worldAccelZ_cs", DoubleType(), False),
        StructField("worldGyroX_cs", DoubleType(), False),
        StructField("worldGyroY_cs", DoubleType(), False),
        StructField("worldGyroZ_cs", DoubleType(), False),
        StructField("temperature_cs", DoubleType(), False),
        StructField("concentric", IntegerType(), True),
        StructField("eccentric", IntegerType(), True),
        StructField("start_end_excercise", IntegerType(), True),
        StructField("start_end_excercise_id", IntegerType(), True),
        StructField("filename", StringType(), True),
        StructField("measure_date", TimestampType(), True),
        StructField("timestamp", StringType(), True),
        StructField("mpuVersion", StringType(), True),
        StructField("excercise_id", IntegerType(), True),
    ]
)
productionSchema = StructType(
    [
        StructField("time_vector", DoubleType(), False),
        StructField("accelX_ws", DoubleType(), False),
        StructField("accelY_ws", DoubleType(), False),
        StructField("accelZ_ws", DoubleType(), False),
        StructField("gyroX_ws", DoubleType(), False),
        StructField("gyroY_ws", DoubleType(), False),
        StructField("gyroZ_ws", DoubleType(), False),
        StructField("quatX_ws", DoubleType(), False),
        StructField("quatY_ws", DoubleType(), False),
        StructField("quatZ_ws", DoubleType(), False),
        StructField("quatW_ws", DoubleType(), False),
        StructField("worldAccelX_ws", DoubleType(), False),
        StructField("worldAccelY_ws", DoubleType(), False),
        StructField("worldAccelZ_ws", DoubleType(), False),
        StructField("worldGyroX_ws", DoubleType(), False),
        StructField("worldGyroY_ws", DoubleType(), False),
        StructField("worldGyroZ_ws", DoubleType(), False),
        StructField("temperature_ws", DoubleType(), False),
        StructField("accelX_cs", DoubleType(), False),
        StructField("accelY_cs", DoubleType(), False),
        StructField("accelZ_cs", DoubleType(), False),
        StructField("gyroX_cs", DoubleType(), False),
        StructField("gyroY_cs", DoubleType(), False),
        StructField("gyroZ_cs", DoubleType(), False),
        StructField("quatX_cs", DoubleType(), False),
        StructField("quatY_cs", DoubleType(), False),
        StructField("quatZ_cs", DoubleType(), False),
        StructField("quatW_cs", DoubleType(), False),
        StructField("worldAccelX_cs", DoubleType(), False),
        StructField("worldAccelY_cs", DoubleType(), False),
        StructField("worldAccelZ_cs", DoubleType(), False),
        StructField("worldGyroX_cs", DoubleType(), False),
        StructField("worldGyroY_cs", DoubleType(), False),
        StructField("worldGyroZ_cs", DoubleType(), False),
        StructField("temperature_cs", DoubleType(), False),
        StructField("filename", StringType(), True),
        StructField("measure_date", TimestampType(), True),
        StructField("timestamp", StringType(), True),
        StructField("mpuVersion", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Start the merge operation
# MAGIC

# COMMAND ----------

dataAnalisysPipeline = (
    UpsertData(
        importhiveDirectoryName='hive_metastore.ser6db',
        exporthiveDirectoryName='hive_metastore.ser6db',
        tableName='bronze_table',
        selectDict=master_signal_dict,
    )
    .createMasterTable(tableName = "training_silver", schema = trainingSchema ,partitionOn = ['filename'])
    .createMasterTable(tableName = "production_silver", schema = productionSchema ,partitionOn = ['filename'])
    .mergeStreamWithMasterTable(
        UpsertClass=Upsert(
            exporthiveDirectoryName='hive_metastore.ser6db.',
            resamplingCond=None,
        ).updateDeltaTable,
        checkpointDirectoryPath="wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/metadata/silver/checkpoint",
        softMaxBytesPerBatch=10 * 1024 * 1024 * 1024,
    )
)

# COMMAND ----------

dbutils.fs.rm('wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/metadata/silver/', True)
if spark.catalog.tableExists('ser6db.training_silver'):
    spark.sql('DROP TABLE ser6db.training_silver')
    spark.sql('DROP TABLE ser6db.production_silver')
