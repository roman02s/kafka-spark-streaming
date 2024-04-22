#!/usr/bin/env python
# coding: utf-8

"""
This script uses PySpark for linear regression analysis. It reads data, preprocesses it,
and then trains and evaluates a linear regression model.
"""

import pickle

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import SparkSession


def init_spark_session():
    """Initialize and return a Spark session"""
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    return spark


def read_data(spark, path, file_name):
    """Read data from a CSV file"""
    return spark.read.csv(f"{path}/{file_name}", inferSchema=True, header=True)


def extract_features(sdf):
    """Extract features from the Spark DataFrame"""
    str_features, int_features = [], []
    for col_name, dtype in sdf.dtypes:
        if dtype == "string":
            str_features.append(col_name)
        else:
            int_features.append(col_name)

    if "SalePrice" in int_features:
        int_features.remove("SalePrice")

    return str_features, int_features


def preprocess_data(sdf, str_features, int_features):
    """Preprocess the data and return a list of stages for the pipeline"""
    stages = []

    # Imputing null values in integer features
    imputer = Imputer(inputCols=int_features, outputCols=int_features)
    stages.append(imputer)

    # Encoding string features
    for feature in str_features:
        indexer = StringIndexer(
            inputCol=feature, outputCol=f"{feature}_Index", handleInvalid="keep"
        )
        stages.append(indexer)

    # Assembling features
    feature_assembler = VectorAssembler(
        inputCols=[f"{f}_Index" for f in str_features] + int_features,
        outputCol="features",
    )
    stages.append(feature_assembler)

    # OneHotEncoder
    one_hot_encoder = OneHotEncoder(
        inputCols=["Neighborhood_Index"], outputCols=["NeighborhoodVec"]
    )
    stages.append(one_hot_encoder)

    # Vector Indexing
    vector_indexer = VectorIndexer(
        inputCol="features", outputCol="indexedFeatures", handleInvalid="keep"
    )
    stages.append(vector_indexer)

    return stages


def build_and_train_model(sdf, stages):
    """Build the pipeline and train the model"""
    # Append the Linear Regression model to the stages
    lr = LinearRegression(
        featuresCol="indexedFeatures",
        labelCol="SalePrice",
        maxIter=10,
        regParam=0.3,
        elasticNetParam=0.8,
        predictionCol="prediction_lr",
    )
    rfr = RandomForestRegressor(
        featuresCol="indexedFeatures",
        labelCol="SalePrice",
        maxBins=350,
        predictionCol="prediction_rfr",
    )
    dtr = DecisionTreeRegressor(
        featuresCol="indexedFeatures",
        labelCol="SalePrice",
        maxBins=350,
        predictionCol="prediction_dtr",
    )
    gbtr = GBTRegressor(
        featuresCol="indexedFeatures", labelCol="SalePrice", maxIter=10, maxBins=350
    )
    stages.append(lr)
    stages.append(rfr)
    stages.append(dtr)
    stages.append(gbtr)

    # Creating a pipeline
    pipeline = Pipeline(stages=stages)

    # Train model
    model = pipeline.fit(sdf)

    return model


def evaluate_model(model, sdf):
    """Evaluate the model and print the Mean Squared Error"""
    predictions = model.transform(sdf)
    evaluator1 = RegressionEvaluator(
        labelCol="SalePrice", predictionCol="prediction", metricName="mse"
    )
    evaluator2 = RegressionEvaluator(
        labelCol="SalePrice", predictionCol="prediction", metricName="r2"
    )
    mse = evaluator1.evaluate(predictions)
    r2 = evaluator2.evaluate(predictions)
    print(f"Mean Squared Error (MSE) = {mse}")
    print(f"R2 = {r2}")


def main():
    spark = init_spark_session()
    path = "."

    # Reading data
    sdf_train = read_data(spark, path, "train.csv")

    # Extracting features
    str_features, int_features = extract_features(sdf_train)

    # Preprocessing data
    stages = preprocess_data(sdf_train, str_features, int_features)

    # Building and training the model
    model = build_and_train_model(sdf_train, stages)

    # Evaluating the model
    model.save("house_price_model.pkl")
    evaluate_model(model, sdf_train)


if __name__ == "__main__":
    main()
