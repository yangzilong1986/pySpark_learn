# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler

spark=SparkSession.builder.\
            appName("logstic").\
            config("spark.master","local").\
            getOrCreate()

schema=""

for i in range(65):
    schema=schema+"_c"+str(i)+" DOUBLE"+","

schema=schema[:len(schema)-1]

df_train=spark.read.csv("./data/optdigits.tra",schema=schema)
df_test=spark.read.csv("./data/optdigits.tes",schema=schema)

cols=[]
for i in range(65):
    cols.append("_c"+str(i))

df_train.head=cols
df_test.head=cols

assembler=VectorAssembler(inputCols=cols[:-1],outputCol="features")

train_output=assembler.transform(df_train)
test_output=assembler.transform(df_test)

train_features=train_output.select("features").toDF("features")
test_features=test_output.select("features").toDF("features")

kmeans=KMeans().setK(value=20).setSeed(1)

model=kmeans.fit(train_features)

predictions=model.transform(test_features)

evaluator=ClusteringEvaluator()

silhouette=evaluator.evaluate(predictions)

print(str(silhouette))

centers=model.clusterCenters()

for center in centers:
    print(center)
