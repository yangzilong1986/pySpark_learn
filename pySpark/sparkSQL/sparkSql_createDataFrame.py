# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import pandas as pd
import numpy as np


def create_spark_sql():
    spark = SparkSession.builder. \
        appName("sparkSql"). \
        config("spark.master", "local"). \
        getOrCreate()

    return spark


def create_dataframe_by_tuple(spark):
    data = [("gavin", 30), ("tom", 40)]
    df = spark.createDataFrame(data, schema=["name", "age"])
    return df


def create_dataframe_by_rdd(spark):
    data = [("gavin", 30), ("tom", 40)]
    sc = spark.sparkContext
    rdd = sc.parallelize(data)
    df = spark.createDataFrame(rdd, schema=["name", "age"])
    return df


def create_dataframe_by_row(spark):
    data = [("gavin", 30), ("tom", 40)]
    sc = spark.sparkContext
    rdd = sc.parallelize(data)
    Person = Row("name", "age")
    person = rdd.map(lambda line: Person(*line))
    df = spark.createDataFrame(person)
    return df


def create_dataframe_by_structtype(spark):
    data = [("gavin", 30), ("tom", 40)]
    sc = spark.sparkContext
    rdd = sc.parallelize(data)
    schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
    df = spark.createDataFrame(rdd, schema)
    return df


def create_dataframe_by_pandas(spark):
    pd_df = pd.DataFrame(np.arange(16).reshape((4, 4)), columns=list("abcd"))
    df = spark.createDataFrame(pd_df)
    return df


def create_dataframe_by_range(spark):
    df = spark.range(0, 1000, 100)
    return df


if __name__ == '__main__':
    spark = create_spark_sql()

    dataFrame = create_dataframe_by_range(spark)
    dataFrame.show()

    spark.stop()
