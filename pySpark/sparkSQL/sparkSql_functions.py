# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *

import numpy as np


def create_spark_sql():
    spark = SparkSession.builder. \
        appName("sparkSql"). \
        config("spark.master", "local"). \
        getOrCreate()
    return spark

def read_file_by_parquet(spark):
    df = spark.read.parquet("./data/users.parquet")
    return df

def read_file_by_csv(spark):
    df = spark.read.csv("./data/customers.csv",header=True)
    return df
"""
    通过agg定义字段以及统计方式
"""
def agg_usecase(df):
    df.agg({"Age":"max"}).show()

"""
    通过alias定义dataFrame别名,别名不能在sql查询时使用,略显鸡肋
"""
def alias_uscase(df):
    df01=df.alias("cus1")
    df02=df.alias("cus2")
    df03=df01.join(df02,"CustomerId","inner")
    df03.show(10)

"""
    显示表的列名称
"""
def columns_usecase(df):
    print(df.columns)

def count_usercase(df):
    print(df.count())

"""
    全局临时表的生命周期为app的整个生命周期
    创建全局临时表,引用时须加上global_temp
"""
def createGlobalTempView_usecase(spark,df):
    df.createGlobalTempView("df")
    spark.sql("select * from global_temp.df limit 10").show()

"""
    临时表生命周期仅限于当前sparkSession
"""
def createTempView_usecase(spark,df):
    df.createTempView("df")
    spark.sql("select * from df limit 10").show()

"""
    cube算子操作类似于数据透视表
"""
def cube_usecase(df):
    df.cube("Age","Genre").\
        count().\
        show(10)

"""
    显示dataFrame各列的主要统计因素
"""
def describe_usecase(df):
    df.describe().show()

"""
    通过drop删除指定列
"""
def drop_usecase(df):
    df.drop("Age").show()

"""
    删除指定列的重复数据
"""
def dropDuplicates_usecase(spark):
    data = [("gavin", 30,188), ("tom", 40,160),("tom",40,170)]
    sc = spark.sparkContext
    rdd = sc.parallelize(data)
    Person = Row("name", "age","height")
    person = rdd.map(lambda line: Person(*line))
    df = spark.createDataFrame(person)
    df.dropDuplicates(subset=["name","age"]).show()

"""
    dropna (how='any', thresh=None, subset=None)删除DataFrame中的na数据，关键字参
    数how指定如何删，有“any”和‘all’两种选项，thresh指定行中na数据有多少个时删除整行数
    据，这个设置将覆盖how关键字参数的设置，subset指定在那几列中删除na数据
"""
def dropna_usecase(spark):
    df=spark.createDataFrame([(np.nan, 27., 170.), (44., 27., 170.),
                                (np.nan, np.nan, 170.)], schema=['luck', 'age', 'weight'])
    df.show()
    df.dropna(how='any').show()
    df.dropna(how='all').show()
    df.dropna(thresh=2).show()

def fillna_usecase(spark):
    df= spark.createDataFrame([(np.nan, 27., 170.), (44., 27., 170.),
                                (np.nan, np.nan, 170.)], schema=['luck', 'age', 'weight'])

    df.fillna(0.0).show()

def filter_usecase(spark):
    df=spark.createDataFrame([(np.nan, 27., 170.), (44., 27., 170.),
                                (np.nan, np.nan, 170.)], schema=['luck', 'age', 'weight'])
    df.filter(df.luck!= np.nan).show()

"""
    底层使用的是rdd的foreach
"""
def foreach_usecase(df):
    def myprint(x):
        print(x.Age)
    df.foreach(myprint)

"""
    按照给定的权重列表对dataFrame进行数据分割
"""
def randomSplit_usecase(df):
    train,test=df.randomSplit([0.5,0.5])
    print(train.count())
    print(test.count())

"""
     sample (withReplacement=None, fraction=None, seed=None),用于从DataFrame中进行
    采样的方法，withReplacement关键字参数用于指定是否采用有放回的采样，true为有放回
    采用，false为无放回的采样，fraction指定采样的比例，seed采样种子，相同的种子对应的
    采样总是相同的，用于场景的复现。
"""
def sample_usecase(df):
    res=df.sample(withReplacement=False, fraction=0.2, seed=1)
    res.show()

"""
    根据指定的列进行数据抽样,指定特征值比例进行数据抽取
"""
def sampleBy_usecase(df):
    df.sampleBy('Genre', {'Male': 0.1, 'Female': 0.15}).show()

def select_usecase(df):
    df.select("Age","Genre").show()

"""
    将dataFrame数据转换为json格式
"""
def toJSON_usecase(df):
    res=df.toJSON()
    res.foreach(print)

"""
    基于原有的数据列创建新的数据列
"""
def withColumn_usecase(df):
    df.withColumn('Age2', df.Age ** 2).show(10)

if __name__ == '__main__':
    spark = create_spark_sql()
    df=read_file_by_csv(spark)

    toJSON_usecase(df)
