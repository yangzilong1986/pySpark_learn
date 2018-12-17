# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import PandasUDFType
from pyspark.sql import Window

"""
    Important classes of Spark SQL and DataFrames:
    - :class:`pyspark.sql.SparkSession`
      Main entry point for :class:`DataFrame` and SQL functionality.
    - :class:`pyspark.sql.DataFrame`
      A distributed collection of data grouped into named columns.
    - :class:`pyspark.sql.Column`
      A column expression in a :class:`DataFrame`.
    - :class:`pyspark.sql.Row`
      A row of data in a :class:`DataFrame`.
    - :class:`pyspark.sql.GroupedData`
      Aggregation methods, returned by :func:`DataFrame.groupBy`.
    - :class:`pyspark.sql.DataFrameNaFunctions`
      Methods for handling missing data (null values).
    - :class:`pyspark.sql.DataFrameStatFunctions`
      Methods for statistics functionality.
    - :class:`pyspark.sql.functions`
      List of built-in functions available for :class:`DataFrame`.
    - :class:`pyspark.sql.types`
      List of data types available.
    - :class:`pyspark.sql.Window`
      For working with window functions.
"""

def createSparkSql():
    spark=SparkSession.builder.\
                appName("sparkSql").\
                config("spark.master","local").\
                getOrCreate()
    return spark

def window_rank_usecase(spark):
    """
        window_rank函数的应用,获取分组函数的排名
        创建Window时需要不能使用rowsBetween
    """
    data = [("a", 10), ("b", 20), ("a", 20), ("a", 30), ("b", 40), ("b", 50)]
    df = spark.createDataFrame(data, schema=['name', 'age'])

    window = Window.partitionBy("name").orderBy("age")

    df01 = df.withColumn("avg",rank().over(window))

    df01.show()


def window_rowsbetween_usecase(spark):
    """
        window_rowsbetween窗口平移函数,可以指定评议范围,计算移动窗口范围内的数据

    """

    data = [("a", 10), ("b", 20), ("a", 20), ("a", 30), ("b", 40), ("b", 50)]
    df = spark.createDataFrame(data, schema=['name', 'age'])

    window = Window.partitionBy("name").orderBy("age").rowsBetween(-1, Window.currentRow)

    df.withColumn("avg",avg("age").over(window)).show()


def group_pandas_udf_usecase(spark):
    """ 运行速度慢,建议谨慎使用"""
    df = spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def normalize(pdf):
        v = pdf.v
        return pdf.assign(v=(v - v.mean()) / v.std())

    df.groupby("id").apply(normalize).show()






if __name__ == '__main__':
    spark=createSparkSql()

    group_pandas_udf_usecase(spark)