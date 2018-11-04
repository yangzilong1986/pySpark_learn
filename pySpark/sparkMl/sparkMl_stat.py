# coding:utf-8

import numpy as np
from pyspark import SparkConf,SparkContext
from pyspark.mllib.stat import Statistics

conf=SparkConf().setAppName("stat").setMaster("local")
sc=SparkContext(conf=conf)

sc.setLogLevel("ERROR")

"""
    基础统计模块
"""
mat=sc.parallelize([np.arange(1,10,2),np.arange(11,20,2),np.arange(21,30,2)],3)

summary=Statistics.colStats(mat)

print(summary)

print(summary.max())

print(summary.min())

print(summary.mean())

print(summary.count())

print(summary.numNonzeros())

print(summary.variance())


"""
  L1是权值向量W中各个元素的绝对值之和
"""
print(summary.normL1())

"""
  L2是2权值向量W中各个元素的平方和再求平方根
"""
print(summary.normL2())


"""
    相关系数统计
"""

# from pyspark.ml.linalg import Vectors
# from pyspark.ml.stat import Correlation
# from pyspark.sql import SparkSession
#
# spark=SparkSession.builder.\
#             appName("local"). \
#             config("spark.master", "local"). \
#             getOrCreate()
#
# data=[(Vectors.sparse(4,[(0,1.0),(3,-2.0)]),),(Vectors.dense([4.0,5.0,0.0,3.0]),),(Vectors.dense([6.0,7.0,0.0,8.0]),),(Vectors.sparse(4,[(0,9.0),(3,1.0)]),)]
# df=spark.createDataFrame(data,["features"])
# r1=Correlation.corr(df,"features").head()
# r2=Correlation.corr(df,"features","spearman").head()
#
# print(u"皮尔生相关系数:\n"+str(r1[0]))
# print(u"斯皮尔曼相关系数:\n"+str(r2[0]))

