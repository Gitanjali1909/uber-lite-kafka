from kafka import KafkaConsumer
import json

def consume_topic(topic, max_messages=50):
    """
    Non-blocking Kafka consumer: fetch up to max_messages and return list.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=500  # exit quickly if no new messages
    )
    msgs = []
    for i, msg in enumerate(consumer):
        msgs.append(msg.value)
        if i >= max_messages - 1:
            break
    consumer.close()
    return msgs
