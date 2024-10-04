from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer

def main():
    string_deserializer = StringDeserializer('utf_8')

    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': string_deserializer,
        'value.deserializer': string_deserializer,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
    }

    consumer = DeserializingConsumer(consumer_config)

    topics = ['topic1', 'topic2']
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(msg.error())
            else:
                process_message(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def process_message(msg):
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    key = msg.key()
    value = msg.value()

    print(f"Received message from {topic} [Partition: {partition}, Offset: {offset}]")
    print(f"Key: {key}, Value: {value}")

    if topic == 'topic1':
        handle_topic1_message(value)
    elif topic == 'topic2':
        handle_topic2_message(value)

def handle_topic1_message(value):
    print(f"Processing message from topic1: {value}")

def handle_topic2_message(value):
    print(f"Processing message from topic2: {value}")

if __name__ == '__main__':
    main()
