
from pySpark.sparkSQL.sparkSql_group import createSparkSql,createDataFrame
from pyspark.sql.functions import *
from pyspark.sql import Window


"""
    columns的核心在于列数据名称及类型的转换
"""

def alias_usecase(df):
    return df.select(df["Age"].alias("age"),df["Income"].alias("income"))

"""
    数据类型转换asType与cast功能一致
"""
def asType_usecase(df):
    return df.select(df["Age"].astype("string"),df["Income"].astype("string"))

""" 
    通过列值的筛选过滤数据,where与filter使用效果等同
    列算子:contains,endwith,eqNullSafe,isNotNull,isNull,isin,
        like,rlike,startWith,substr
    以上算子使用方法相近
"""

def contains_usecase(spark):
    df = spark.createDataFrame([(1, 2), (2, 3), (3, 4)], schema=['a', 'b'])
    return df.where(df["a"].contains(2))

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




if __name__ == '__main__':
    spark=createSparkSql()

    window_rowsbetween_usecase(spark)



