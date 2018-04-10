#!/usr/bin/env python
import threading, logging, time, json

from kafka import KafkaConsumer, KafkaProducer
from db import Session, update_denormalized_view

kafka_server="kafka"
kafka_port=9092

print("#### Starting kafka-postgres-debezium-denormalize-demo ####")

kafka_url=kafka_server+":"+ str(kafka_port)

class CDCConsumer():

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers= kafka_url,
                                 auto_offset_reset='earliest')
        consumer.subscribe(pattern='dbserver1.public.*')

        for message in consumer:
            data = json.loads(message.value)
            pkey = json.loads(message.key)
            tablename = message.topic.split(".")[-1]
            envelope = data['payload']

            update_denormalized_view(Session(), pkey, tablename, envelope)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    consumer = CDCConsumer()
    consumer.run()
