
from sparkSql_group import createSparkSql,createDataFrame
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


if __name__ == '__main__':
    spark=createSparkSql()
    df=createDataFrame(spark)

    contains_usecase(spark).show()

