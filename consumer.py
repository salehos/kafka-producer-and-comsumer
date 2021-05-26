from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import json

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

c = consumer("51.178.160.66:9092", 1, "log-test")

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue


    message = msg.value().decode('utf-8')
    message = json.loads(message)
    print('Received message: {}'.format(message))
