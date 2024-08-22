# Databricks notebook source
from pyspark.ml.classification import DecisionTreeClassifier ,RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder  
from pyspark.ml.feature import VectorAssembler ,StandardScaler  
from pyspark.ml import Pipeline
import mlflow

# COMMAND ----------

df_master = spark.table("ser6db.training_silver").orderBy("measure_date", "time_vector")

feature_cols = [  
    'accelX_cs', 'accelX_ws', 'accelY_cs', 'accelY_ws',   
    'accelZ_cs', 'accelZ_ws', 'gyroX_cs', 'gyroX_ws',   
    'gyroY_cs', 'gyroY_ws', 'gyroZ_cs', 'gyroZ_ws',   
    'quatW_cs', 'quatW_ws', 'quatX_cs', 'quatX_ws',   
    'quatY_cs', 'quatY_ws', 'quatZ_cs', 'quatZ_ws',   
    'temperature_cs', 'temperature_ws', 'time_vector',   
    'worldAccelX_cs', 'worldAccelX_ws', 'worldAccelY_cs',   
    'worldAccelY_ws', 'worldAccelZ_cs', 'worldAccelZ_ws',   
    'worldGyroX_cs', 'worldGyroX_ws', 'worldGyroY_cs',   
    'worldGyroY_ws', 'worldGyroZ_cs', 'worldGyroZ_ws'  
]

dtc_params = {
    "maxBins": 24,
    "maxDepth": 16,
    "minInfoGain": 0.05,
    "minInstancesPerNode": 4,
    "impurity": "entropy",
}

rfc_params = {  
    'numTrees': 50,  
    'maxDepth': 15,  
    'maxBins': 20,  
    'minInfoGain': 0.001,  
    'impurity': 'entropy'
}  

# COMMAND ----------

def dtc_model_test(df, label_column, feature_cols, params):  
    with mlflow.start_run(nested=True) as mlflow_run:  
        mlflow.sklearn.autolog(  
            disable=False,  
            log_input_examples=True,  
            silent=True,  
            exclusive=False)  
  
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")  
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)  
 
        dcr = DecisionTreeClassifier(labelCol=label_column, featuresCol="scaledFeatures",   
                                      maxDepth=int(params['maxDepth']),   
                                      minInstancesPerNode=int(params['minInstancesPerNode']),  
                                      maxBins=int(params['maxBins']),  
                                      minInfoGain=params['minInfoGain'],  
                                      impurity=params['impurity'])  
  
        pipeline = Pipeline(stages=[assembler, scaler, dcr])  
  
        distinct_filenames = df.select("filename").distinct()  
        train_filenames, test_filenames = distinct_filenames.randomSplit([0.8, 0.2], seed=42)  
  
        train_dataframe = df.join(train_filenames, on="filename", how="inner")  
        test_dataframe = df.join(test_filenames, on="filename", how="inner")  
  
        paramGrid = ParamGridBuilder().build()  
        crossval = CrossValidator(estimator=pipeline,  
                                   estimatorParamMaps=paramGrid,  
                                   evaluator=MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="f1"),  
                                   numFolds=5)  
  
        cv_model = crossval.fit(train_dataframe)  
  
        predictions = cv_model.transform(test_dataframe)  
  
        accuracy = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy").evaluate(predictions)  
        precision = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)  
        recall = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)  
        f1_score = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="f1").evaluate(predictions)  
  
        print(f"Accuracy : {accuracy} , Precision : {precision} , Recall : {recall} , f1_score : {f1_score} ")

        mlflow.log_metric("accuracy", accuracy)  
        mlflow.log_metric("precision", precision)  
        mlflow.log_metric("recall", recall)  
        mlflow.log_metric("f1_score", f1_score)  
        
        return pipeline


# COMMAND ----------

def rfc_model_test(df, label_column, feature_cols, params):  
    with mlflow.start_run(nested=True) as mlflow_run:  
        mlflow.sklearn.autolog(  
            disable=False,  
            log_input_examples=True,  
            silent=True,  
            exclusive=False  
        )  

        mlflow.log_params(params)
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")  
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)  
  
        rfc = RandomForestClassifier(  
            labelCol=label_column,  
            featuresCol="scaledFeatures",  
            numTrees=int(params['numTrees']),  
            maxDepth=int(params['maxDepth']),  
            maxBins=int(params['maxBins']),  
            minInfoGain=params['minInfoGain'],  
            impurity=params['impurity']  
        )  
  
        pipeline = Pipeline(stages=[assembler, scaler, rfc])  
  
        distinct_filenames = df.select("filename").distinct()  
        train_filenames, test_filenames = distinct_filenames.randomSplit([0.8, 0.2], seed=42)  
  
        train_dataframe = df.join(train_filenames, on="filename", how="inner")  
        test_dataframe = df.join(test_filenames, on="filename", how="inner")  
  
        paramGrid = ParamGridBuilder().build()  
        crossval = CrossValidator(  
            estimator=pipeline,  
            estimatorParamMaps=paramGrid,  
            evaluator=MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="f1"),  
            numFolds=5  
        )  
  
        cv_model = crossval.fit(train_dataframe)  
        predictions = cv_model.transform(test_dataframe)  
  
        accuracy = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy").evaluate(predictions)  
        precision = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)  
        recall = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)  
        f1_score = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="f1").evaluate(predictions)  
  
        mlflow.log_metric("accuracy", accuracy)  
        mlflow.log_metric("precision", precision)  
        mlflow.log_metric("recall", recall)  
        mlflow.log_metric("f1_score", f1_score)     

        return pipeline


# COMMAND ----------

with mlflow.start_run(run_name="Current_models") as parent_run: 
    pipleine = dtc_model_test(df_master , "start_end_excercise_id" , feature_cols , dtc_params)
 
    final_model = pipleine.fit(df_master.select(feature_cols + ["start_end_excercise_id"]))

    mlflow.spark.log_model(final_model, "start-end-excercise-id-model")  

    feature_cols = feature_cols + ["start_end_excercise_id"]
    pipleine = rfc_model_test(df_master , "concentric" , feature_cols , rfc_params)

    final_model = pipleine.fit(df_master.select(feature_cols + ["concentric"]))

    mlflow.spark.log_model(final_model, "concentric-model")  

    pipleine = rfc_model_test(df_master , "eccentric" , feature_cols , rfc_params)

    final_model = pipleine.fit(df_master.select(feature_cols + ["eccentric"]))

    mlflow.spark.log_model(final_model, "eccentric-model") 

