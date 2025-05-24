import json
from kafka import KafkaProducer

def getConnection():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producer created!")
    return producer

def closeConnection(producer):
    if producer:
        producer.close()
        print("Producer closed!")
