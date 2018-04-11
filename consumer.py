#!/usr/bin/env python
import threading, logging, time, json
from datetime import datetime as dt

from kafka import KafkaConsumer, KafkaProducer
from db import Session, update_denormalized_view_batch

kafka_server="kafka"
kafka_port=9092

print("#### Starting kafka-postgres-debezium-denormalize-demo ####")

kafka_url=kafka_server+":"+ str(kafka_port)

class CDCConsumer():

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=kafka_url,
                                 group_id='denormalize-data-group')
        consumer.subscribe(['dbserver1.public.companies', 'dbserver1.public.people', 'dbserver1.public.users'])
        db = Session()

        while True:
            chunk = consumer.poll(timeout_ms=1000)
            if chunk:
                update_denormalized_view_batch(db, chunk)
                db.commit()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    consumer = CDCConsumer()
    consumer.run()
