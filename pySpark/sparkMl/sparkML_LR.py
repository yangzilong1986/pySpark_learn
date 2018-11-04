# coding:utf-8

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

spark=SparkSession.builder.\
            appName("logstic").\
            config("spark.master","local").\
            getOrCreate()

training=spark.read.\
        format("libsvm").\
        load("./data/lib_svm.txt")

"""
    maxIter=>模型训练迭代次数
    regPram=>正则化强度
    elasticNetParam=>L1正则和L2正则影响的权重
"""
lr=LogisticRegression(maxIter=10,regParam=3,elasticNetParam=0.8,family="multinomial")


lrModel=lr.fit(training)

# print("Coefficients:"+str(lrModel.coefficients))
# print("Intercept:"+str(lrModel.intercept))

traininfSummary=lrModel.summary



objectiveHistory=traininfSummary.objectiveHistory

for objective in objectiveHistory:
    print(objective)

traininfSummary.roc.show()












spark.stop()

