# -*- coding:utf-8 -*-

from kafka import KafkaProducer
from datetime import datetime
import time
from py_spark.sparkStructStream import common


def tobytes(line):
    return bytes(line, encoding="utf-8")


def send_message_group():
    producer = KafkaProducer(bootstrap_servers=common.KAFKA_BROKET_LIST)

    for i in range(100):
        key = tobytes(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        value = tobytes("hadoop" if i % 3 == 0 else "hive")

        print(str(key) + "," +str(value))

        producer.send("group_withwatermark", key=key, value=value)

        time.sleep(2)

    producer.flush()

    producer.close()




def send_message_stream_join():
    producer = KafkaProducer(bootstrap_servers=common.KAFKA_BROKET_LIST)


    for i in range(100):
        key = tobytes(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


        value01 = tobytes(str(i))
        value02 = tobytes(str(100-i))

        print(str(key)+","+str(value01)+","+str(value02))

        producer.send("structStream01", key=key, value=value01)
        time.sleep(1)
        # producer.send("structStream02", key=key, value=value02)


    producer.flush()

    producer.close()


if __name__ == '__main__':
    send_message_stream_join()




