from confluent_kafka import Consumer, KafkaException
import json
import time

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

def create_tracer(service_name):
    tracer_provider = TracerProvider(resource=Resource.create({SERVICE_NAME:service_name}))
    trace.set_tracer_provider(tracer_provider)
    otlp_exporter = OTLPSpanExporter(endpoint="http://jaeger-collector.observability.svc.cluster.local:4317",insecure=True)
    span_processor = SimpleSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

# tracer = trace.get_tracer(__name__)

message = {"resourceSpans": [{"resource": {"attributes": [{"key": "container.id", "value": {"stringValue": "6a7cc39635b509bf70a45a1d694cfa52cd645898cd768696292089d407734e40"}}, {"key": "host.arch", "value": {"stringValue": "amd64"}}, {"key": "host.name", "value": {"stringValue": "java-app-67859cd7c-6qvk2"}}, {"key": "k8s.container.name", "value": {"stringValue": "spring"}}, {"key": "k8s.deployment.name", "value": {"stringValue": "java-app"}}, {"key": "k8s.namespace.name", "value": {"stringValue": "java-app"}}, {"key": "k8s.node.name", "value": {"stringValue": "rke2embedcare-embedcaretestw-c8f1688a-kkf82"}}, {"key": "k8s.pod.name", "value": {"stringValue": "java-app-67859cd7c-6qvk2"}}, {"key": "k8s.replicaset.name", "value": {"stringValue": "java-app-67859cd7c"}}, {"key": "os.description", "value": {"stringValue": "Linux 5.4.0-192-generic"}}, {"key": "os.type", "value": {"stringValue": "linux"}}, {"key": "process.command_args", "value": {"arrayValue": {"values": [{"stringValue": "/usr/local/openjdk-17/bin/java"}, {"stringValue": "-jar"}, {"stringValue": "/app/app.jar"}]}}}, {"key": "process.executable.path", "value": {"stringValue": "/usr/local/openjdk-17/bin/java"}}, {"key": "process.pid", "value": {"intValue": "1"}}, {"key": "process.runtime.description", "value": {"stringValue": "Oracle Corporation OpenJDK 64-Bit Server VM 17+35-2724"}}, {"key": "process.runtime.name", "value": {"stringValue": "OpenJDK Runtime Environment"}}, {"key": "process.runtime.version", "value": {"stringValue": "17+35-2724"}}, {"key": "service.name", "value": {"stringValue": "java-app"}}, {"key": "service.version", "value": {"stringValue": "latest"}}, {"key": "telemetry.auto.version", "value": {"stringValue": "1.32.1"}}, {"key": "telemetry.sdk.language", "value": {"stringValue": "java"}}, {"key": "telemetry.sdk.name", "value": {"stringValue": "opentelemetry"}}, {"key": "telemetry.sdk.version", "value": {"stringValue": "1.34.1"}}, {"key": "k8s.pod.ip", "value": {"stringValue": "10.42.67.177"}}, {"key": "app", "value": {"stringValue": "java-app"}}, {"key": "pod-template-hash", "value": {"stringValue": "67859cd7c"}}, {"key": "k8s.pod.start_time", "value": {"stringValue": "2024-09-13T06:57:39Z"}}, {"key": "k8s.pod.uid", "value": {"stringValue": "ed9d4b99-968d-4bf9-9657-d19b784fe73f"}}]}, "scopeSpans": [{"scope": {"name": "io.opentelemetry.servlet-3.0", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "92987e75278ac376", "parentSpanId": "dfa63309c297c11a", "name": "HttpServletResponseWrapper.sendRedirect", "kind": 1, "startTimeUnixNano": "1726603145022094673", "endTimeUnixNano": "1726603145022154425", "attributes": [{"key": "code.namespace", "value": {"stringValue": "javax.servlet.http.HttpServletResponseWrapper"}}, {"key": "thread.id", "value": {"intValue": "64"}}, {"key": "code.function", "value": {"stringValue": "sendRedirect"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}]}, {"scope": {"name": "io.opentelemetry.spring-data-1.8", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "6df445f2f5fb426b", "parentSpanId": "8723511231ab9bf2", "name": "OwnerRepository.save", "kind": 1, "startTimeUnixNano": "1726603145015791598", "endTimeUnixNano": "1726603145021379123", "attributes": [{"key": "code.namespace", "value": {"stringValue": "org.springframework.samples.petclinic.owner.OwnerRepository"}}, {"key": "thread.id", "value": {"intValue": "64"}}, {"key": "code.function", "value": {"stringValue": "save"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "b79acd4fb3b31c3a", "parentSpanId": "1c840d0f9be24dea", "name": "OwnerRepository.findById", "kind": 1, "startTimeUnixNano": "1726603145097131707", "endTimeUnixNano": "1726603145098968949", "attributes": [{"key": "code.namespace", "value": {"stringValue": "org.springframework.samples.petclinic.owner.OwnerRepository"}}, {"key": "thread.id", "value": {"intValue": "63"}}, {"key": "code.function", "value": {"stringValue": "findById"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}]}, {"scope": {"name": "io.opentelemetry.hibernate-4.0", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "95d247663ef67e03", "parentSpanId": "6df445f2f5fb426b", "name": "Session.persist org.springframework.samples.petclinic.owner.Owner", "kind": 1, "startTimeUnixNano": "1726603145016688845", "endTimeUnixNano": "1726603145020414624", "attributes": [{"key": "thread.id", "value": {"intValue": "64"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "1bf2a6df9784efdf", "parentSpanId": "6df445f2f5fb426b", "name": "Transaction.commit", "kind": 1, "startTimeUnixNano": "1726603145020709382", "endTimeUnixNano": "1726603145021251277", "attributes": [{"key": "thread.id", "value": {"intValue": "64"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "fee9a537753473c8", "parentSpanId": "b79acd4fb3b31c3a", "name": "SELECT", "kind": 1, "startTimeUnixNano": "1726603145097685189", "endTimeUnixNano": "1726603145098629740", "attributes": [{"key": "thread.id", "value": {"intValue": "63"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "f2c4369c3d28ccbc", "parentSpanId": "b79acd4fb3b31c3a", "name": "Transaction.commit", "kind": 1, "startTimeUnixNano": "1726603145098790637", "endTimeUnixNano": "1726603145098862659", "attributes": [{"key": "thread.id", "value": {"intValue": "63"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}]}, {"scope": {"name": "io.opentelemetry.jdbc", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "d8563bd55e2d7e86", "parentSpanId": "95d247663ef67e03", "name": "INSERT b8648f98-a4a8-4e83-b545-66d653586585.owners", "kind": 3, "startTimeUnixNano": "1726603145019820956", "endTimeUnixNano": "1726603145020166354", "attributes": [{"key": "db.user", "value": {"stringValue": "sa"}}, {"key": "db.connection_string", "value": {"stringValue": "h2:mem:"}}, {"key": "thread.id", "value": {"intValue": "64"}}, {"key": "db.system", "value": {"stringValue": "h2"}}, {"key": "db.statement", "value": {"stringValue": "insert into owners (id, first_name, last_name, address, city, telephone) values (null, ?, ?, ?, ?, ?)"}}, {"key": "db.operation", "value": {"stringValue": "INSERT"}}, {"key": "db.sql.table", "value": {"stringValue": "owners"}}, {"key": "db.name", "value": {"stringValue": "b8648f98-a4a8-4e83-b545-66d653586585"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "544a61a1278eae4c", "parentSpanId": "fee9a537753473c8", "name": "SELECT b8648f98-a4a8-4e83-b545-66d653586585", "kind": 3, "startTimeUnixNano": "1726603145097906617", "endTimeUnixNano": "1726603145098168745", "attributes": [{"key": "db.user", "value": {"stringValue": "sa"}}, {"key": "db.connection_string", "value": {"stringValue": "h2:mem:"}}, {"key": "thread.id", "value": {"intValue": "63"}}, {"key": "db.system", "value": {"stringValue": "h2"}}, {"key": "db.statement", "value": {"stringValue": "select owner0_.id as id1_0_0_, pets1_.id as id1_1_1_, owner0_.first_name as first_na2_0_0_, owner0_.last_name as last_nam3_0_0_, owner0_.address as address4_0_0_, owner0_.city as city5_0_0_, owner0_.telephone as telephon6_0_0_, pets1_.name as name2_1_1_, pets1_.birth_date as birth_da3_1_1_, pets1_.owner_id as owner_id4_1_1_, pets1_.type_id as type_id5_1_1_, pets1_.owner_id as owner_id4_1_0__, pets1_.id as id1_1_0__ from owners owner0_ left outer join pets pets1_ on owner0_.id=pets1_.owner_id where owner0_.id=?"}}, {"key": "db.operation", "value": {"stringValue": "SELECT"}}, {"key": "db.name", "value": {"stringValue": "b8648f98-a4a8-4e83-b545-66d653586585"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}]}, {"scope": {"name": "io.opentelemetry.tomcat-7.0", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "9845ae6411af52f4", "parentSpanId": "", "name": "POST /owners/new", "kind": 2, "startTimeUnixNano": "1726603145006933966", "endTimeUnixNano": "1726603145022899374", "attributes": [{"key": "net.sock.peer.port", "value": {"intValue": "14508"}}, {"key": "net.sock.host.port", "value": {"intValue": "8080"}}, {"key": "net.host.name", "value": {"stringValue": "127.0.0.1"}}, {"key": "user_agent.original", "value": {"stringValue": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"}}, {"key": "thread.id", "value": {"intValue": "64"}}, {"key": "http.target", "value": {"stringValue": "/owners/new"}}, {"key": "net.sock.peer.addr", "value": {"stringValue": "192.168.233.77"}}, {"key": "http.response_content_length", "value": {"intValue": "0"}}, {"key": "net.host.port", "value": {"intValue": "8181"}}, {"key": "http.status_code", "value": {"intValue": "302"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}, {"key": "http.request_content_length", "value": {"intValue": "73"}}, {"key": "http.route", "value": {"stringValue": "/owners/new"}}, {"key": "net.sock.host.addr", "value": {"stringValue": "10.42.67.177"}}, {"key": "net.protocol.name", "value": {"stringValue": "http"}}, {"key": "net.protocol.version", "value": {"stringValue": "1.1"}}, {"key": "http.scheme", "value": {"stringValue": "http"}}, {"key": "http.method", "value": {"stringValue": "POST"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "873f44e70bffffd3", "parentSpanId": "", "name": "GET /owners/{ownerId}", "kind": 2, "startTimeUnixNano": "1726603145095747426", "endTimeUnixNano": "1726603145126257188", "attributes": [{"key": "net.sock.peer.port", "value": {"intValue": "14508"}}, {"key": "net.sock.host.port", "value": {"intValue": "8080"}}, {"key": "net.host.name", "value": {"stringValue": "127.0.0.1"}}, {"key": "user_agent.original", "value": {"stringValue": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"}}, {"key": "thread.id", "value": {"intValue": "63"}}, {"key": "http.target", "value": {"stringValue": "/owners/12"}}, {"key": "net.sock.peer.addr", "value": {"stringValue": "192.168.233.77"}}, {"key": "net.host.port", "value": {"intValue": "8181"}}, {"key": "http.status_code", "value": {"intValue": "200"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}, {"key": "http.route", "value": {"stringValue": "/owners/{ownerId}"}}, {"key": "net.sock.host.addr", "value": {"stringValue": "10.42.67.177"}}, {"key": "net.protocol.name", "value": {"stringValue": "http"}}, {"key": "net.protocol.version", "value": {"stringValue": "1.1"}}, {"key": "http.scheme", "value": {"stringValue": "http"}}, {"key": "http.method", "value": {"stringValue": "GET"}}], "status": {}}]}, {"scope": {"name": "io.opentelemetry.spring-webmvc-3.1", "version": "1.32.1-alpha"}, "spans": [{"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "8723511231ab9bf2", "parentSpanId": "9845ae6411af52f4", "name": "OwnerController.processCreationForm", "kind": 1, "startTimeUnixNano": "1726603145008460908", "endTimeUnixNano": "1726603145021523068", "attributes": [{"key": "thread.id", "value": {"intValue": "64"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "b6851ed7b095fa1cb913086ad674fca7", "spanId": "dfa63309c297c11a", "parentSpanId": "9845ae6411af52f4", "name": "Render redirect:/owners/12", "kind": 1, "startTimeUnixNano": "1726603145021552455", "endTimeUnixNano": "1726603145022163437", "attributes": [{"key": "thread.id", "value": {"intValue": "64"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-9"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "1c840d0f9be24dea", "parentSpanId": "873f44e70bffffd3", "name": "OwnerController.showOwner", "kind": 1, "startTimeUnixNano": "1726603145096872560", "endTimeUnixNano": "1726603145099455510", "attributes": [{"key": "thread.id", "value": {"intValue": "63"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}, {"traceId": "2050eeaecb7dce44f4d8f94e3c2a74ef", "spanId": "f7bc76367f26c669", "parentSpanId": "873f44e70bffffd3", "name": "Render owners/ownerDetails", "kind": 1, "startTimeUnixNano": "1726603145099486305", "endTimeUnixNano": "1726603145125830618", "attributes": [{"key": "thread.id", "value": {"intValue": "63"}}, {"key": "thread.name", "value": {"stringValue": "http-nio-8080-exec-8"}}], "status": {}}]}], "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"}]}

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

def inject_trace_data_into_tracer(trace_data):
    resource_data = trace_data['resource']['attributes']
    resource_attributes = {}
    for attr in resource_data:
        resource_attributes = resource_attributes | {attr['key']:attr['value'].get('stringValue') or attr['value'].get('intValue') or attr['value'].get('arrayValue')}
        if attr['key'] == 'service.name':
            service_name = attr['value'].get('stringValue')
            create_tracer(service_name)
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

def main():
    # while True:
    #     msg = consumer.poll(timeout=1.0)
    #     if msg is None:
    #         continue
    #     if msg.error():
    #         if msg.error().code() == KafkaError._PARTITION_EOF: 
    #             continue 
    #         else:
    #             raise KafkaException(msg.error())
    #     message = json.loads(msg.value().decode('utf-8'))
    #     for trace_data in message['resourceSpans']:
    #         inject_trace_data_into_tracer(trace_data)
    #     time.sleep(1)

    for trace_data in message['resourceSpans']:
        inject_trace_data_into_tracer(trace_data)

if __name__ == "__main__":
    main()
