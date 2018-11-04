# -*- coding:utf-8 -*-

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SparkFiles

import os
import numpy as np
from operator import gt

"""
    pysoark关键算子    
"""

def generateSparkContext():
    conf=SparkConf()
    conf.set("master","local")

    sc=SparkContext(conf=conf)
    return sc

def accumulator_usecase(sc):
    """
        sparkContext自定义累加器,累加器可以与sparkStreaming的upstateByKey算子进行对比
                获取新数据,在原有数据基础上进行累加
    """
    acc=sc.accumulator(0)

    def acc_add(a):
        acc.add(a)
        return a

    rdd=sc.parallelize(np.arange(100)).\
            map(acc_add).\
            collect()

    print(acc.value)


def addFiles_usecase(sc):
    """
        addFiles方法将本地路径的文件上传到集群中,供运算过程中各个node节点下载数据，
        上传的文件使用SparkFile.get(filename)获取,方法可以用于配置文件的获取,共享数据
        集的获取

        配置参数的获取可以同样可以通过定义一个python文件来设置,python文件会在程序提交过程
        中在每个worker中复制一份,相关参数可以直接通过配置文件获取
    """
    tempdir="F:\python\pySpark_learn\pySpark\sparkCore"
    path=os.path.join(tempdir,"num_data")
    with open(path,"w") as f:
        f.write("100")

    sc.addFile(path)

    def fun(iterable):
        with open(SparkFiles.get("num_data")) as f:
            value=int(f.readline())
            return [x*value for x in iterable]

    sc.parallelize(np.arange(10),2).\
                mapPartitions(fun).\
                foreach(print)


def broadcast_usecase(sc):
    """
        广播变量的使用
        desroty() 释放广播变量,该方法会阻塞直到所有广播变量释放掉为止.该方法调用之后,广播变量就不能使用
        unpersist(blocking=False) 该方法用于释放executor上存储的广播变量,用于释放executor内存资源,但driver节点的广播变量不会释放
    """
    broad=sc.broadcast("hello")

    rdd=sc.parallelize(range(9),3).\
        map(lambda line:broad.value+str(line)).\
        foreach(print)

    print("applicationId:",sc.applicationId)


def setLogLevel_usecase(sc):
    """
        日志级别:ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    """

    sc.setLogLevel("DEBUG")

    sc.parallelize(range(10)).\
        foreach(print)


def getAll_usecase():
    """
        getAll方法获取sparkConf中所有的配置
    """
    conf=SparkConf()

    print(conf.getAll())


def second_sort_usecase(sc):
    """
        二次排序
    """
    class MySort():
        def __init__(self, list):
            self.rdd = sc.parallelize(list)

        @staticmethod
        def _toGirl(x):
            return Girl(x[0], x[1], x[2])

        def sort(self, ascending=True):
            return self.rdd.sortBy(self._toGirl, ascending)

    class Girl():
        def __init__(self, name, score, age):
            self.name = name
            self.score = score
            self.age = age

        def __gt__(self, other):
            if other.score == self.score:
                return gt(self.age, other.age)
            else:
                return gt(self.score, other.score)

        def __repr__(self):
            return self.name

    l = [('crystal', 90, 22), ('crystal1', 100, 28), ('crystal3', 100, 22)]

    s = MySort(l)
    print(s.sort(ascending=False).collect())

if __name__ == '__main__':

    sc=generateSparkContext()

    # accumulator_usecase(sc)
    # addFiles_usecase(sc)
    # broadcast_usecase(sc)
    # setLogLevel_usecase(sc)
    getAll_usecase()

    sc.stop()


