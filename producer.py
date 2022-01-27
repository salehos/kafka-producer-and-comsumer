import confluent_kafka
import json
from requests.exceptions import ConnectTimeout
from decouple import config
KAFKA_SERVERS = config("KAFKA_SERVERS")
KAFKA_TOPIC = config("KAFKA_TOPIC")

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))

def send_to_kafka(kafka_servers, json_message, kafka_topic):
    p = confluent_kafka.Producer({'bootstrap.servers': kafka_servers})
    p.poll(0)
    p.produce(kafka_topic,json.dumps(json_message),callback=delivery_report)
    try:
        re = p.flush(timeout=10)
        if re > 0:
            raise ConnectTimeout
    except:
        raise ConnectTimeout
    
# in second arg of send_to_kafka function you must send your json message
send_to_kafka(KAFKA_SERVERS, {"Hello" : "World"}, KAFKA_TOPIC)