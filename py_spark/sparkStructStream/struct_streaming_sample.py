# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from py_spark.sparkStructStream import common



def get_spark_instance():
    spark = SparkSession.builder. \
        appName("kafka"). \
        config("spark.jars.packages", common.KAFKA_DEFDENCE). \
        getOrCreate()

    return spark


def kafka_structstreaming_group_withwatermark():
    """
        withWatermark:水印标记,水印标记的时间表示数据存留时间,超过存留时间,数据自动消失
        window:数据滑动窗口,两个参数,第一个参数是窗口时间长度,第二个窗口是滑动时间长度

        outputMode:输出模式append,update,complete
            append模式:append模式在有watermarrk标记分组计算的情景下无法触发,原因不明
                 Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark

            update模式:update模式输出,输入新数据对应的数据

            complete模式:complete是全量输出,complete模式输出必须有聚合操作,complete模式默认保留所有历史数据,代码中可以添加watermark,程序可以正常运行,但是历史数据不会失效
                'Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;

        trigger:数据接收间隔,等同于saprkSteaming的数据处理间隔
            1,once = True 数据处理一次程序结束
            2,processingTime 对指定范围内数据进行计算（最小100ms）
            3,continuous 目前处于实现阶段，数据延迟在1ms

    """
    spark = get_spark_instance()

    df = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", common.KAFKA_BROKET_LIST). \
        option("subscribe", "group_withwatermark"). \
        load()

    res = df.select(from_utc_timestamp(df["key"].cast("string"), 'GMT+0').alias("timestamp"),
                    df["value"].cast("string").alias("data"))

    res = res. \
        withWatermark('timestamp', '10 seconds').\
        groupby(
            window(res["timestamp"], "10 seconds", "5 seconds"),
            res["data"]). \
        count()

    query = res.writeStream. \
        outputMode("complete"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start(truncate=False)

    query.awaitTermination()



def kafka_structstreaming_stream_join():

    """
        数据流join
            数据流join只支持append,update模式输出不支持complete模式输出
            数据流join必须指定数据失效时间(watermark)，如果是inner模式,两条流都必须是缓存流
                如果是外连接,被连接一侧必须是缓存流,另外一侧是可选项


        inner(如果不指定关联模式,默认为inner,inner模式必须保证两边的数据流都进行watermark标记)
            只支持append模式输出,inner模式join条件可以只指定关联的key,时间纬度的条件是可选项
             expr(
                  left = right
                    )
              )

            expr(
                  left = right AND
                  lefttime >= righttime AND
                  lefttime <= righttime + interval 5 minute
                    )
              )

        leftOuter,rightOuter模式支持append,update模式输出,外部关联必须包含时间纬度条件
            外部连接leftOuter模式至少要保证右侧数据为watermark缓存数据,左侧数据为可选项
                   rightOuter模式至少要保证左侧数据为watermark缓存数据,右侧数据为可选项
            'Stream-stream outer join between two streaming DataFrame/Datasets is not supported
            without a watermark in the join keys, or a watermark on the nullable side and an appropriate range condition;
                如果对时间间隔只有单边判断,如下图所示,单边判断必须与外连接方式保持一致(个人猜测避免缓存数据与join流数据时间冲突)
                lefttime >= righttime 对应 leftOuter; lefttime<=righttime 对应rightOuter
             expr(
                  left = right AND
                  lefttime >= righttime
                    )
                  leftOuter
              )
                如果对时间间隔有两边判断,如下图所示,两种外连接方式均可(同时限制了缓存数据的join范围)
                数据测试结果:

              expr(
                  left = right AND
                  lefttime >= righttime AND
                  lefttime <= righttime + interval 5 minute
                    )
                  leftOuter
              )


    """

    spark = get_spark_instance()

    df01 = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", common.KAFKA_BROKET_LIST). \
        option("subscribe", "structStream01"). \
        load()

    df02 = spark.readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", common.KAFKA_BROKET_LIST). \
        option("subscribe", "structStream02"). \
        load()


    res01 = df01.select(from_utc_timestamp(df01["key"].cast("string"), 'GMT+0').alias("lefttime"),
                      df01["value"].cast("string").alias("left"))

    res02 = df02.select(from_utc_timestamp(df02["key"].cast("string"), 'GMT+0').alias("righttime"),
                      df02["value"].cast("string").alias("right"))


    res03 =res01

    res04 =res02.withWatermark('righttime', '20 minute')


    res =res03.join(
        res04,
        expr("""
             left = right AND
             lefttime >= righttime AND
             lefttime <= righttime + interval 5 minute
             """),
            "rightOuter"
         )

    query = res.writeStream. \
        outputMode("update"). \
        format("console"). \
        trigger(processingTime='5 seconds'). \
        start(truncate=False)

    query.awaitTermination()




if __name__ == '__main__':
    kafka_structstreaming_stream_join()