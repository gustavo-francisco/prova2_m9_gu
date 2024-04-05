from mockafka import FakeConsumer, FakeProducer
import json
from pymongo import MongoClient

message = {
    "idSensor": "sensor_001",
    "timestamp": "2024-04-04T12:34:56Z",
    "tipoPoluente": "PM2.5",
    "nivel": 35.2
}

topic = "test_prova"

def integrity():
    producer = FakeProducer()
    consumer = FakeConsumer()
    producer.produce(
        topic=topic,
        value=message
    )
    msg = consumer.poll(timeout=0.1)
    new_message = msg.value().decode("utf-8")
    new_message = new_message.replace("'", "\"")
    assert json.loads(new_message) == message

def persistence():
    producer = FakeProducer()
    consumer = FakeConsumer()
    producer.produce(
        topic=topic,
        value=message
    )
    msg = consumer.poll(timeout=0.1)
    new_message = msg.value().decode("utf-8")
    new_message = new_message.replace("'", "\"")
    colection.insert_one({'test_msg':new_message})

    client = MongoClient('mongodb://localhost:27017/')
    db = client['provaM9']
    colection = db['prova-gu']
    assert colection.find_one(message) == message