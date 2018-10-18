# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from pyspark.sql.group import *
from pyspark.sql.utils import *
from pyspark.sql.catalog import *
from pyspark.sql.column import *

def createSparkSql():
    spark=SparkSession.builder.\
                appName("sparkSql").\
                config("spark.master","local").\
                getOrCreate()
    return spark

def createDataFrame(spark):
    df=spark.read.csv("./data/customers.csv",header=True)
    res=alias_usecase(df)
    return res

"""
   dataFrame列格式转换(数据类型转换,列别名定义) 
"""
def alias_usecase(df):
    res= df.select(df["CustomerID"],df["Genre"],\
                   df["Age"].cast("int").alias("age"),\
                   df["Annual Income (k$)"].cast("int").alias("Income"),\
                   df["Spending Score (1-100)"].cast("int").alias("score"))
    return res

def agg_usecase(df):
    return df.groupBy("Genre").agg({"Age":"mean","Income":"max","score":"min"})
"""
    avg,count,max,mean,min,sum等算子用法相同
    1,选取指定的数据列
    2,指定分组列
    3,指定选取列的计算方式
"""
def avg_usecase(df):
    return df.select(df["Genre"],df["Age"],df["score"]).groupBy("Genre").avg()



if __name__ == '__main__':
    spark=createSparkSql()

    df=createDataFrame(spark)

    res=avg_usecase(df)

    res.show()
