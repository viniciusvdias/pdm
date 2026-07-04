import json
import time
import os
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'wikimediaRecentchange')
WIKIMEDIA_URL = os.environ.get('WIKIMEDIA_URL', 'https://stream.wikimedia.org/v2/stream/recentchange')
PRODUCER_SOURCE = os.environ.get('PRODUCER_SOURCE', 'live').strip().lower()
WIKIMEDIA_SAMPLE_PATH = os.environ.get(
    'WIKIMEDIA_SAMPLE_PATH',
    '/sample/wikimedia-recentchange-sample.txt',
)
SAMPLE_DELAY_SECONDS = float(os.environ.get('PRODUCER_SAMPLE_DELAY_SECONDS', '0'))


def send_event(producer, event_data):
    producer.send(TOPIC_NAME, value=event_data)
    print(f"Message sent -> Article: {event_data.get('title')}", flush=True)

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Successful connection to Kafka at {KAFKA_BOOTSTRAP_SERVERS}!", flush=True)
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... Error: {e}", flush=True)
            time.sleep(3)


def replay_sample():
    producer = create_producer()
    print(f"Starting local sample replay: {WIKIMEDIA_SAMPLE_PATH}", flush=True)
    print(f"Kafka Topic: {TOPIC_NAME}", flush=True)

    if not os.path.exists(WIKIMEDIA_SAMPLE_PATH):
        raise FileNotFoundError(f"Sample file not found: {WIKIMEDIA_SAMPLE_PATH}")

    with open(WIKIMEDIA_SAMPLE_PATH, 'r', encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith('data: '):
                continue

            try:
                event_data = json.loads(line[6:])
                send_event(producer, event_data)
                if SAMPLE_DELAY_SECONDS > 0:
                    time.sleep(SAMPLE_DELAY_SECONDS)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error sending sample message to Kafka: {e}", flush=True)

    producer.flush()
    print('Sample replay completed. Keeping container idle for inspection.', flush=True)
    while True:
        time.sleep(3600)

def stream_wikimedia():
    if PRODUCER_SOURCE == 'sample':
        replay_sample()
        return

    producer = create_producer()

    print(f"Starting Wikimedia stream capture: {WIKIMEDIA_URL}", flush=True)
    print(f"Kafka Topic: {TOPIC_NAME}", flush=True)
    headers = {
        'User-Agent': 'FinalProjectBigDataUFLA/1.0'
    }
    
    while True:
        try:
            print("Establishing HTTP connection with Wikimedia...", flush=True)
            response = requests.get(WIKIMEDIA_URL, stream=True, headers=headers, timeout=30)
            print(f"Connection established! Status Code: {response.status_code}", flush=True)
            
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data: '):
                        try:
                            data_content = decoded_line[6:]
                            event_data = json.loads(data_content)

                            send_event(producer, event_data)
                        except json.JSONDecodeError:
                            pass
                        except Exception as e:
                            print(f"Error sending message to Kafka: {e}", flush=True)
        except Exception as e:
            print(f"Error in connection with Wikimedia: {e}. Restarting in 5 s...", flush=True)
            time.sleep(5)

if __name__ == '__main__':
    stream_wikimedia()