from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import json
import time

kafka_config = {
        'bootstrap.servers':'kafka-release-controller-0.kafka-release-controller-headless.kafka.svc.cluster.local:9092',
        'group.id': 'common',
        'auto.offset.reset': 'earliest'
        }

consumer =  Consumer(kafka_config)
consumer.subscribe(['otlp_spans','otlp_metrics','otlp_logs'])

es = Elasticsearch(
        ['https://elasticsearch-master-0.elasticsearch-master-headless.elastic.svc.cluster.local:9200'],
        ca_certs="/etc/tls/ca.crt",
        client_cert="/etc/tls/tls.crt",
        client_key="/etc/tls/tls.key",
        verify_certs=True,
        basic_auth=('elastic','TSNPWP7EsVqpoH8q')
    )

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
        message = msg.value.decode('utf-8')
        print(message)
        resp = es.index(index='telemetry_data',body={'telemetry_data':message})
        #resp = es.indices.create(index='telemetry_data',ignore=400)
        #print(resp['result'])
        time.sleep(1)

if __name__ == "__main__":
    main()
