# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import StructType

"""
    输出模式:
        complete 
            1,数据全量输出,数据不断追加
            2,complete输出模式必须有聚合操作
            3,complete默认保留所有历史数据,
                代码可以添加watermasrk,
                程序执行的过程中watermark自动失效不会删除过期数据
        append,
            1 数据新增,只有新增的数据才触发计算(append模式的聚合操作需要设置watermark,目前在pyspark中添加了watermark依旧无法执行,原因待查 )
        update 
            2 以无边界表的方式不断追加数据,每次输出只是输出表中出现更改的数据,update模式对聚合以及watermark支持最好
                        
    时间间隔方式:
        1,once = True 数据处理一次,相当于离线数据处理
        2,processingTime 对指定范围内数据进行计算（最小100ms）
        3,continuous 目前处于实现阶段，保证数据延迟在1ms
    
    数据关联
        数据流之间的关联必须指定数据失效时间(watermark),同时需要指定数据流关联的时间差
        
          expr(
              left = right AND
              lefttime >= righttime AND
              lefttime <= righttime + interval 5 minute
                ),
    
             "leftOuter"
         )
        
        innerjoin 只支持append模式输出,append模式join只关联最新的数据,新的数据会把旧数据覆盖
        leftOuter,rightOuter模式支持append,update模式输出 
                  流动的流会不断更新,关联对象流会进行状态缓存,update模式相同的key不同的时间戳的数据不会替换,数据join时会有多条结果
        
        数据关联不支持complete模式
        
    算子:map类操作,from_utc_timestamp(df01["key"].cast("string") 调用function直接作用于选中的数据列
        flamap类操作,explode(split(lines.value, " ")),通过explode将列表数据转换为多行数据
"""
def test_socket():
    spark = SparkSession.builder. \
        appName("StructNetworkWordCount"). \
        getOrCreate()

    # 数据源:socket,kafka,file
    """
        socket类型数据源不支持schema格式转换
    """
    lines = spark.readStream. \
        format("socket"). \
        option("host", "localhost"). \
        option("port", 9999). \
        load()

    lines.printSchema()

    # explode 等同于flatmap算子,将列表扩充为多行
    # dataFrame基础算子
    # words = lines.select(
    #     explode(
    #         split(lines.value, " ")
    #     )
    # )

    words = lines.select((split(lines["value"], " ")[0]).alias("name"),
                         (split(lines["value"], " ")[1]).cast("int").alias("age"))

    words = words.groupby("name").max("age")

    words.printSchema()

    """ 对已有字段进行数据类型转换,列别名设置 """
    # wordCount = words.select(words["col"].cast("int").alias("word"))

    # wordCounts = wordCount.select((wordCount["word"]*2).alias("num"),wordCount["word"])

    """ 通过已有字段添加新字段 """
    # wordCounts = wordCount.withColumn("num", wordCount["word"]*2)

    # """ 注册表格,通过sql方式对表格进行操作"""
    # wordCounts.createTempView("table")
    # table = spark.sql("select num,word,current_date as data_time from table ")

    # groupBy("word"). \
    # count()

    # table.printSchema()

    query = words.writeStream. \
        outputMode("complete"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start()

    query.awaitTermination()


def test_csv():
    spark = SparkSession.builder. \
        appName("StructNetworkWordCount"). \
        getOrCreate()

    userSchema = StructType().add("name", "string").add("age", "integer")

    csvDF = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv("./data")  # Equivalent to format("csv").load("/path/to/directory")

    csvDF.printSchema()

    query = csvDF.writeStream. \
        outputMode("update"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start()

    query.awaitTermination()


def test_kafka():
    spark = SparkSession.builder. \
        appName("kafka"). \
        config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2"). \
        getOrCreate()

    df = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094"). \
        option("subscribe", "structStream"). \
        load()

    # res = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    res = df.select(from_utc_timestamp(df["key"].cast("string"),'GMT+0').alias("timestamp"), df["value"].cast("string").alias("data"))

    # res = res.select(res["timestamp"],json_tuple(res["data"],"a","b").alias("v1","v2"))
    # withWatermark("timestamp","5 seconds").\
    res = res. \
        withWatermark('timestamp', '10 seconds').\
        groupby(
        window(res["timestamp"], "10 seconds", "5 seconds"),
        res["data"]). \
        count()

    # res = res. \
    #     withWatermark('timestamp', '10 seconds'). \
    #     groupby("data").count()
    # trigger(processingTime='5 seconds'). \
    query = res.writeStream. \
        outputMode("append"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start(truncate=False)

    query.awaitTermination()

def test_kafka_join():

    spark = SparkSession.builder. \
        appName("kafka"). \
        config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2"). \
        getOrCreate()

    df01 = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094"). \
        option("subscribe", "structStream01"). \
        load()

    df02 = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094"). \
        option("subscribe", "structStream02"). \
        load()


    res01 = df01.select(from_utc_timestamp(df01["key"].cast("string"), 'GMT+0').alias("lefttime"),
                    df01["value"].cast("string").alias("left"))

    res02 = df02.select(from_utc_timestamp(df02["key"].cast("string"), 'GMT+0').alias("righttime"),
                      df02["value"].cast("string").alias("right"))


    res03 =res01.withWatermark('lefttime', '10 minute')

    res04 =res02.withWatermark('righttime', '20 minute')

    res =res03.join(
        res04,
        expr("""
         left = right AND
         lefttime >= righttime AND
         lefttime <= righttime + interval 5 minute
         """),
        "leftOuter"

         )

    query = res.writeStream. \
        outputMode("append"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start(truncate=False)

    query.awaitTermination()




if __name__ == '__main__':
    test_kafka_join()
    