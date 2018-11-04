# coding:utf-8

from pyspark.mllib.linalg import SparseVector,Matrices,Matrix
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf,SparkContext
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg.distributed import IndexedRowMatrix,IndexedRow


conf=SparkConf().setAppName("data").setMaster("local")
sc=SparkContext(conf=conf)


"""
    lab01为密集向量,lab02为稀疏向量
"""
# lab01=LabeledPoint(1.0,[1.0,2.0,3.0])
#
# print(lab01)
#
# lab02=LabeledPoint(0.0,SparseVector(3,[0,2],[1.0,3.0]))
#
# print(lab02)


"""
   mat01为密集矩阵,mat02为稀疏矩阵 
"""

# mat01=Matrices.dense(2,3,[1.0,2.0,3.0,4.0,5.0,6.0])
# print(mat01.toArray())
#
# mat02=Matrices.sparse(3,2,[0,1,2],[0,2,1],[2.0,3.0,4.0])
# print(mat02.toArray())


"""
    面向行的分布式矩阵RowMatrix
"""

rows=sc.parallelize([[1,2,3],[2,3,4],[3,4,5]],3)
mat=RowMatrix(rows)

print(mat.numRows())
print(mat.numCols())


"""
    IndexedRowMatrix加索引的行矩阵
"""


"""
    CoordinateMatrix基于坐标的矩阵
"""

"""
    BlockMatrix 分块矩阵
"""





