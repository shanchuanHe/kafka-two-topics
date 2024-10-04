from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for Message {msg.key()}: {err}")
    else:
        print(f"Message {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    string_serializer = StringSerializer('utf_8')

    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': string_serializer,
        'value.serializer': string_serializer,
    }

    producer = SerializingProducer(producer_config)

    try:
        for i in range(10):
            key = str(i)
            value_topic1 = f"Message {i} for topic1"
            value_topic2 = f"Message {i} for topic2"

            # Send messages to topic1
            producer.produce(
                topic='topic1',
                key=key,
                value=value_topic1,
                on_delivery=delivery_report
            )

            # Send messages to topic2
            producer.produce(
                topic='topic2',
                key=key,
                value=value_topic2,
                on_delivery=delivery_report
            )

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # Flush any remaining messages
        producer.flush()

if __name__ == '__main__':
    main()
