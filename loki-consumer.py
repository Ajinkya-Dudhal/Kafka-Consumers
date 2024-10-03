from confluent_kafka import Consumer, KafkaException
import requests
import json
import time

kafka_config = {
        'bootstrap.servers':'kafka-release-controller-0.kafka-release-controller-headless.kafka.svc.cluster.local:9092', 
        'group.id': 'logs',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'admin',
        'sasl.password': 'admin123'
        }

consumer =  Consumer(kafka_config)
consumer.subscribe(['otlp_logs'])

LOKI_URL = 'http://loki.loki.svc.cluster.local:3100/loki/api/v1/push'

def convert_to_loki_format(message):
    streams = []

    log_details = message['resourceLogs']

    labels = {}
    for log_data in log_details:
        for attr in log_data['resource']['attributes']:
            attr['key'] = attr['key'].replace(".","_")
            attr['key'] = attr['key'].replace("-","_")
            labels = labels | {attr['key']:attr['value']['stringValue']}
            
        for log_records in log_data['scopeLogs']:
            for log_record in log_records['logRecords']:
                timestamp = log_record['timeUnixNano']
                attributes = {attr['key']:attr['value']['stringValue'] for attr in log_record['attributes']}
                log = {"body": log_record['body']['stringValue'],"attributes":attributes,"resources":labels}
                log = json.dumps(log)

                stream = {
                    "stream": labels,
                    "values": [[str(timestamp),log]]
                }
                streams.append(stream)
    
    payload = {"streams": streams}
    print(payload)

    response = requests.post(LOKI_URL,headers = {'Content-Type': 'application/json'},json=payload)
    print(response)
    if response.status_code == 204:
        print("Logs sent successfully !!")
    else:
        print(f"Failed to send logs: {response.text}")

def main():
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: 
                continue 
            else:
                raise KafkaException(msg.error())
        message = json.loads(msg.value().decode('utf-8'))
        convert_to_loki_format(message)
        time.sleep(1)

if __name__ == "__main__":
    main()
