from confluent_kafka import Consumer
import json
from decouple import config
KAFKA_SERVERS = config("KAFKA_SERVERS")
KAFKA_TOPIC = config("KAFKA_TOPIC")
KAFKA_GROUP_ID = config("KAFKA_GROUP_ID")

def consumer(servers, group, topic):
    c = Consumer({
        'bootstrap.servers': servers,
        'group.id': group,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })
    c.subscribe([topic])
    return c

c = consumer(KAFKA_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC)

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue


    message = msg.value().decode('utf-8')
    message = json.loads(message)
    print('Received message: {}'.format(message))