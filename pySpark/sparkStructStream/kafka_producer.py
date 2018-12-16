# -*- coding:utf-8 -*-

from kafka import KafkaProducer
from datetime import datetime
import time
import json


def tobytes(line):
    return bytes(line, encoding="utf-8")


def send_message():
    producer = KafkaProducer(bootstrap_servers="115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094",
                             retries=10, api_version_auto_timeout_ms=60000)

    for i in range(100):
        key = tobytes(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # value = tobytes(json.dumps({"a": i, "b": i}))

        # res = "hadoop" if i % 3 == 0 else "hive"

        value01 = tobytes(str(i))
        value02 = tobytes(str(100-i))

        print(str(key)+","+str(value01)+","+str(value02))

        producer.send("structStream01", key=key, value=value01)
        time.sleep(1)
        producer.send("structStream02", key=key, value=value02)

        # time.sleep(1)

    producer.flush()

    producer.close()


if __name__ == '__main__':
    send_message()






