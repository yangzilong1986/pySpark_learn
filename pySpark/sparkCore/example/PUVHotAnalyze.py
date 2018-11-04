# coding:utf-8

from pyspark import SparkConf,SparkContext
import sys

class SparkUtil(object):

    @staticmethod
    def initSparkConf(self,isLocal,appName):
        conf=SparkConf()
        conf.setAppName(appName)
        if isLocal is True:
            conf.setMaster("local")
        return conf

    @staticmethod
    def initSparkContext(self,conf):
        return SparkContext(conf=conf)

    @staticmethod
    def createRDD(self,isLocal,appName,inputPath):
        conf=SparkUtil.initSparkConf(isLocal,appName)
        sc=SparkUtil.initSparkContext(conf)
        return sc.textFile(inputPath)

class PUVAnalyzer(object):

    @staticmethod
    def pvAnalyzer(self,isLocal,inputPath):
        rdd=SparkUtil.createRDD(isLocal,"pvAnalyzer",inputPath)
        rdd.filter(lambda x:x.split("\t")[5] != "register")\
            .map(lambda x:(x.split("\t")[3],1))\
            .reduceByKey(lambda x,y:x+y)\
            .foreach(print)

if __name__ == '__main__':
    conf=SparkConf().setMaster("local").setAppName("pvAnalyzer")
    sc=SparkContext(conf=conf)

    rdd=sc.textFile("./data/input/userLog")

    """
        计算浏览pv
    """
    # rdd.filter(lambda x: x.split("\t")[5] != "register") \
    #     .map(lambda x: (x.split("\t")[3], 1)) \
    #     .reduceByKey(lambda x, y: x + y) \
    #     .foreach(print)

    """
        计算每个页面的uv
        计算逻辑:1,过滤去除空值
                2,获取(pageId,userId)元祖
                3,对元祖进行去重
                4,使用countByKey计算每个pageId的uv数
    """

    # def getPUV(x):
    #     line=x.split("\t")
    #     return (line[3],line[2])
    # res=rdd.filter(lambda x:x.split("\t")[2] != 'null')\
    #     .map(getPUV)\
    #     .distinct()\
    #     .countByKey()\
    # # 通过格式化的方式,自动将不同的数据类型转换为字符串格式
    # for k,v in res.items():
    #     print("pageId:%s,uv:%s" %(k,v))

    """
        计算热门频道Top5活跃用户
            计算逻辑：1，计算出热门频道,对热门频道进行广播
                     2,根据计算出来的热门频道对数据进行过滤
                     3，对过滤数据进行转换,转换为(userId,channel)元祖,进行groupBYKey()操作
                     4，使用flatMap算子对数据进行转换,生成（channel,userid_count）元祖，对元祖进行groupBYKey操作
                     5，对userid_count列表进行遍历操作，计算出top数据
            计算逻辑的核心在于,先通过userId进行分组,然后通过channel分组,避免直接通过channel分组造成数据的大量积累造成内存溢出
    """

    filterRDD=rdd.filter(lambda x:x.split("\t")[2] != "null")\

    # 获取top3的热门频道
    hotChannel=filterRDD.map(lambda x:(x.split("\t")[4],1))\
            .reduceByKey(lambda x,y:x+y)\
            .sortBy(lambda x:x[1],ascending=False)\
            .map(lambda x:x[0])\
            .take(3)

    list=sc.broadcast(hotChannel)

    # 获取用户id,用户id对应的频道集合组成的元祖,返回频道与用户id对应的列表,一对多
    def getUserChannel(x):
        userId=x[0]
        channels=x[1]
        channelCount={}
        res=[]

        for channel in channels:
            if(channel in channelCount):
                channelCount[channel]=channelCount[channel]+1
            else:
                channelCount[channel]=1

        for channel,count in channelCount.items():
            res.append((channel,"%s_%d" % (userId,count)))

        return res

    def getTop5ActiveUser(x):
        channel=x[0]
        user_counts=x[1]

        sort=["","","","",""]

        for user_count in user_counts:
            splite=user_count.split("_")
            userId=splite[0]
            count=int(splite[1])
            for i in range(0,len(sort)):
                if sort[i]=="":
                    sort[i]=user_count
                    break
                elif count>int(sort[i].split("_")[1]):
                    for j in range(len(sort)-1,i,-1):
                        sort[j]=sort[j-1]
                    sort[i]=user_count
                    break
        return channel,sort

    filterRDD.filter(lambda x:list.value.__contains__(x.split("\t")[4]))\
            .map(lambda x:(x.split("\t")[3],x.split("\t")[4]))\
            .groupByKey()\
            .flatMap(getUserChannel)\
            .groupByKey()\
            .map(getTop5ActiveUser)\
            .foreach(print)










