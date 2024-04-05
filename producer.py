from confluent_kafka import Producer, Consumer, KafkaError
from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017/')
client.server_info()
db = client['provaM9']
colection = db['prova-gu']

consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'client.id': 'python-producer'
}

producer = Producer(**producer_config)

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

class NicolaMessage():
    def __init__(self, idSensor, timestamp, tipoPoluente, nivel):
        self.id = idSensor
        self.timestamp = timestamp
        self.type = tipoPoluente
        self.level = nivel
    
    def filter(self):
        return str(f'idSensor:{self.id}, timestamp:{self.timestamp}, tipoPoluente:{self.type}, nivel:{self.level}')

topic = 'qualidadeAr'

file = open('data.json')
message = json.load(file)

new_message = NicolaMessage(message['idSensor'], message['timestamp'], message['tipoPoluente'], message['nivel']).filter()

producer.produce(topic, new_message.encode('utf-8'), callback=delivery_callback)

producer.flush()

consumer = Consumer(**consumer_config)

consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message: {msg.value().decode("utf-8")}')
        colection.insert_one({'msg':msg.value().decode("utf-8")})
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
