from confluent_kafka import Consumer, KafkaException
import json
import time

import threading
import queue
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource,SERVICE_NAME

kafka_config = {
        'bootstrap.servers':'kafka-release-controller-0.kafka-release-controller-headless.kafka.svc.cluster.local:9092', 
        'group.id': 'test-consumer-group',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'admin',
        'sasl.password': 'admin123',
        'auto.offset.reset': 'earliest'
        }

consumer =  Consumer(kafka_config)
consumer.subscribe(['otlp_spans'])

JAEGER_URL = "http://jaeger-collector.observability.svc.cluster.local:4317"

service_threads = {}
service_queues = {}

def start_service_thread():
    pass

def create_tracer(service_name):
    tracer_provider = TracerProvider(resource=Resource.create({SERVICE_NAME:service_name}))
    trace.set_tracer_provider(tracer_provider)
    otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_URL,insecure=True)
    span_processor = SimpleSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

class TreeNode:
    def __init__(self,span_id,parent_id,span_data,attributes):
        self.span_id = span_id
        self.parent_id = parent_id
        self.span_data = span_data
        self.attributes = attributes
        self.left = None
        self.right = None

def send_traces_to_jaeger(node,traceId):
    if node is None:
        return
    print(node.span_id,node.span_data['name'])
    
    with trace.get_tracer(__name__).start_as_current_span(node.span_data['name']) as span:
        name = node.span_data['name'],
        span_context = trace.SpanContext(trace_id=int(traceId,16),span_id=int(node.span_id,16),is_remote=False),
        kind = node.span_data['kind'],
        trace_id = int(traceId,16),
        span_id = int(node.span_id,16),
        parent_id = node.parent_id,
        start_time = node.span_data['start_time'],
        end_time = node.span_data['end_time'],
        for key,value in node.attributes.items():
            span.set_attribute(str(key),str(value))
        send_traces_to_jaeger(node.left,traceId)
        send_traces_to_jaeger(node.right,traceId)

def find_parent(root_node,span):
    if root_node == None:
        return
    if root_node.span_id == span[1]:
        new_node = TreeNode(span[0],span[1],span[2],span[3])
        if root_node.left == None:
            root_node.left = new_node
        else:
            root_node.right = new_node
        return
    find_parent(root_node.left,span)
    find_parent(root_node.right,span)

def create_Tree(dic):
    for trace_id in dic:
        print(trace_id)
        spans = dic[trace_id]
        for span in spans:
            if span[1]=='':
                root = span[0]
                root_node = TreeNode(root,span[1],span[2],span[3])
                spans.remove(span)
                break
        curr_node = root_node
        while len(spans)>0:
            found = False
            for span in spans:
                if curr_node.span_id == span[1]:
                    found = True
                    new_node = TreeNode(span[0],span[1],span[2],span[3])
                    if curr_node.left == None:
                        curr_node.left = new_node
                    else:
                        curr_node.right = new_node
                    curr_node = new_node
                    spans.remove(span)
                    break
            if found == False:
                for span in spans:
                    find_parent(root_node,span)
                    spans.remove(span)
                    break
        send_traces_to_jaeger(root_node,trace_id)

def inject_trace_data_into_tracer(trace_data,service_name):
    create_tracer(service_name)
    resource_data = trace_data['resource']['attributes']
    resource_attributes = {}
    for attr in resource_data:
        resource_attributes = resource_attributes | {attr['key']:attr['value'].get('stringValue') or attr['value'].get('intValue') or attr['value'].get('arrayValue')}
    scopeSpans = trace_data['scopeSpans']
    
    dic = {}
    for span_data in scopeSpans:
        for span in span_data['spans']:
            name = span['name']
            kind = span['kind']
            start_time = span['startTimeUnixNano']
            end_time = span['endTimeUnixNano']
            trace_id = span['traceId']
            span_id = span['spanId']
            parent_id = span['parentSpanId']
            span_attributes = {attr['key']:attr['value'].get('stringValue') or attr['value'].get('intValue') for attr in span.get('attributes')}
            attributes = resource_attributes | span_attributes
            if trace_id in dic:
                dic[trace_id].append([span_id,parent_id,{'name':span['name'],'kind':span['kind'],'start_time':span['startTimeUnixNano'],'end_time':span['endTimeUnixNano']},attributes])
            else:
                dic[trace_id] = [[span_id,parent_id,{'name':span['name'],'kind':span['kind'],'start_time':span['startTimeUnixNano'],'end_time':span['endTimeUnixNano']},attributes]]
    create_Tree(dic)

def create_thread(msg_queue,service_name):
    while True:
        trace_data = msg_queue.get()
        if trace_data is None:
            service_threads.pop(service_name)
            service_queues.pop(service_name)
            break
        print(f"{service_name} processing trace...")
        inject_trace_data_into_tracer(trace_data,service_name)
        print()
        time.sleep(1)

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
        for trace_data in message['resourceSpans']:
            resource_data = trace_data['resource']['attributes']
            for attr in resource_data:
                if attr['key'] == 'service.name':
                    service_name = attr['value'].get('stringValue')
                    # print(service_name)
                    if service_name not in service_queues:
                        q = queue.Queue()
                        q.put(trace_data)
                        service_queues[service_name] = q
                        service_threads[service_name] = threading.Thread(target=create_thread,args=(q,service_name))
                        service_threads[service_name].start()
                        service_threads[service_name].join()
                    else:
                        service_queues[service_name].put(trace_data)
                    break
        time.sleep(1)

if __name__ == "__main__":
    main()
