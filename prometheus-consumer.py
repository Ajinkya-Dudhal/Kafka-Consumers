from confluent_kafka import Consumer, KafkaException
from prometheus_client import Gauge, Counter, Summary, Histogram, start_http_server, CollectorRegistry
import json
import time

kafka_config = {
        'bootstrap.servers':'kafka-release-controller-0.kafka-release-controller-headless.kafka.svc.cluster.local:9092',
        'group.id': 'traces'
        # 'auto.offset.reset': 'earliest'
        }

consumer =  Consumer(kafka_config)
consumer.subscribe(['otlp_metrics'])

registry = CollectorRegistry()

prom_metrics = {}

def convert_metrics_to_prometheus_format(message):
    for resource_metric in message['resourceMetrics']:
        labels = {}
        for attr in resource_metric['resource']['attributes']:
            attr['key'] = attr['key'].replace(".","_")
            attr['key'] = attr['key'].replace("-","_")
            resource_labels = {attr['key']:attr['value'].get('stringValue') or attr['value'].get('intValue') or attr['value'].get('arrayValue')}
            labels = labels | resource_labels

        scope_metrics = resource_metric['scopeMetrics']
        for scope_metric in scope_metrics:
            for metric in scope_metric['metrics']:
                name = metric['name'].replace(".","_")
                description = ''
                if 'description' in metric:
                    description = metric['description']
                # labels = resource_labels

                metric_types = ['gauge','sum','counter','histogram']
                for metric_type in metric_types:
                    if metric_type in metric.keys():
                        for dp in metric[metric_type]['dataPoints']:    
                            if 'attributes' in dp:
                                metric_labels = {attr['key'].replace(".","_"):attr['value'].get('stringValue') or attr['value'].get('intValue') or attr['value'].get('boolValue') for attr in dp['attributes']}
                                labels = labels | metric_labels
                            
                            metric_value = 0
                            if 'asInt' in dp.keys():
                                metric_value = int(dp.get('asInt'))
                            if 'asDouble' in dp.keys():
                                metric_value = float(dp.get('asDouble'))
                            
                            if metric_type == 'gauge':
                                if name not in prom_metrics:
                                    prom_metrics[name] = Gauge(name,description,labelnames=list(labels.keys()))
                                prom_metrics[name].labels(**labels).set(metric_value)
                                
                            elif metric_type == 'sum':
                                if name not in prom_metrics:
                                    prom_metrics[name] = Summary(name,description,labelnames=list(labels.keys()))
                                prom_metrics[name].labels(**labels).observe(metric_value)

                            elif metric_type == 'histogram':
                                buckets = dp.get('explicitBounds')
                                if buckets is None:
                                    continue
                                bucket_counts = [int(count) for count in dp['bucketCounts']]
                                # sum_value = dp['sum']
                                # count = int(dp['count'])
                                if name not in prom_metrics:
                                    prom_metrics[name] = Histogram(name,description,labelnames=list(labels.keys()),buckets=buckets)
                                
                                # for metric_family in registry.collect():
                                #     if metric_family.name == 'http_client_request_duration':
                                #         print(metric_family.name)
                                #         for sample in metric_family.samples:
                                #             # print(sample.labels)
                                #             for labels in sample.labels:
                                #                 print(labels)
                                #                 print()

                                for i in range(len(bucket_counts)):
                                    if bucket_counts[i] > 0:
                                        if i == len(bucket_counts)-1:
                                            bucket_bound = buckets[i-1] - buckets[i - 2]
                                            prom_metrics[name].labels(**labels).observe(bucket_bound)
                                        else:
                                            bucket_bound = buckets[i] if i==0 else buckets[i] - buckets[i - 1]
                                            prom_metrics[name].labels(**labels).observe(bucket_bound)

    print(prom_metrics)
    return prom_metrics

def main():
    start_http_server(8000)

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
        # print(message)
        prom_metrics = convert_metrics_to_prometheus_format(message)
        time.sleep(1)

if __name__ == "__main__":
    main()
