import json
from kafka import KafkaConsumer
import threading
import time

class KafkaDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topics=['rides', 'eta']):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.consumers = {}
        self.data_buffers = {topic: [] for topic in topics}
        self.lock = threading.Lock()
        self.running = False

    def _create_consumer(self, topic):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'streamlit-group-{topic}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def _consume_topic(self, topic):
        consumer = self._create_consumer(topic)
        for message in consumer:
            if not self.running:
                break
            with self.lock:
                self.data_buffers[topic].append(message.value)
                # Keep only the last 200 messages for performance
                self.data_buffers[topic] = self.data_buffers[topic][-200:]
        consumer.close()

    def start_consuming(self):
        if self.running:
            return

        self.running = True
        self.threads = []
        for topic in self.topics:
            thread = threading.Thread(target=self._consume_topic, args=(topic,))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

    def stop_consuming(self):
        self.running = False
        for thread in self.threads:
            thread.join(timeout=1) # Wait for threads to finish

    def get_latest_data(self, topic):
        with self.lock:
            data = list(self.data_buffers[topic])
            self.data_buffers[topic].clear() # Clear buffer after reading
            return data

# Example usage (for testing purposes, not directly used in Streamlit app)
if __name__ == "__main__":
    consumer = KafkaDataConsumer()
    consumer.start_consuming()
    try:
        while True:
            rides_data = consumer.get_latest_data('rides')
            eta_data = consumer.get_latest_data('eta')
            if rides_data:
                print(f"Received {len(rides_data)} rides messages.")
            if eta_data:
                print(f"Received {len(eta_data)} ETA messages.")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping consumer...")
        consumer.stop_consuming()
