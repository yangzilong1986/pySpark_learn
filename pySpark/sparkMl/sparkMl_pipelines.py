# coding:utf-8

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

spark=SparkSession.builder.\
            appName("logstic").\
            config("spark.master","local").\
            getOrCreate()

# 样本训练数据
training=spark.createDataFrame([
    (1.0,Vectors.dense(0.0,1.1,0.1)),
    (0.0,Vectors.dense(2.0,1.0,-1.0)),
    (0.0,Vectors.dense(2.0,1.3,1.0)),
    (1.0,Vectors.dense(0.0,1.2,-0.5))
],["label","features"])

#创建训练模型,设置训练模型参数
lr=LogisticRegression()

"""
    maxIter->模型迭代次数
    regPram->正则化系数
    threshold->临界值
    probablityCol->概率分布列
"""
paramMap={lr.maxIter:10,lr.regParam:0.1,lr.threshold:0.55,lr.probabilityCol:"p"}


print("LogisticRegression parameters:\n"+lr.explainParams()+"\n")

# 使用逻辑回归模型训练数据
model=lr.fit(training,params=paramMap)



print("Model 1 was fit using parameters: ")
print(model.extractParamMap())

test=spark.createDataFrame([(1.0,Vectors.dense([-1.0,1.5,1.3])),(0.0,Vectors.dense([3.0,2.0,-0.1])),(1.0,Vectors.dense([0.0,2.2,-1.5]))],["label","features"])

#训练好的模型可以直接通过transform进行预测
prediction=model.transform(test)

prediction.show()
# result=prediction.select("features","label","p","prediction").collect()
# for row in result:
#     print("features=%s, label=%s -> prob=%s, prediction=%s" %(row.features,row.label,row.p,row.prediction))
