from grpc import ssl_channel_credentials, insecure_channel
from datetime import timedelta
import time
from grpc_interceptor import ClientInterceptor,  ClientCallDetails
import protocol.service_pb2 as pb2
import protocol.service_pb2_grpc as servicegrpc

class MetadataInterceptor(ClientInterceptor):
    def __init__(self, metadata):
        self.metadata = metadata

    def intercept(self, request, metadata, client_call_details, next):
        metadata.update(self.metadata)
        new_client_call_details = ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return next(request, new_client_call_details)


class RpcClient:
    CONNECT_TIMEOUT_MILLIS = 3*1000
    GRPC_MAX_MESSAGE_SIZE = 2*31 - 1
    def __init__(self, endpoints, sslEnabled):
        channel_options = [
            ('grpc.max_send_message_length', -1),
            ('grpc.max_receive_message_length', -1),
            ('grpc.keepalive_time_ms', 1000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.connect_timeout_ms', self.CONNECT_TIMEOUT_MILLIS),
        ]
        if sslEnabled:
            ssl_creds = ssl_channel_credentials()
            self.channel = Channel(endpoints.getGrpcTarget(), ssl_creds, options=channel_options)
        else:
            self.channel = insecure_channel(endpoints.getGrpcTarget(), options=channel_options)

        self.activityNanoTime = time.monotonic_ns()

    def get_stub(self, metadata):
        interceptor = MetadataInterceptor(metadata)

        interceptor_channel = grpc.intercept_channel(self.channel, interceptor)
        stub = servicegrpc.MessagingServiceStub(interceptor_channel)
        return stub

    def __del__(self):
        self.channel.close()
    
    def idle_duration(activity_nano_time):
        return timedelta(microseconds=(time.monotonic_ns() - activity_nano_time) / 1000)
    
    async def query_route(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.QueryRoute(request, deadline=duration)

    async def heartbeat(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.Heartbeat(request, deadline=duration)

    async def send_message(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.SendMessage(request, deadline=duration)

    async def query_assignment(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.QueryAssignment(request, deadline=duration)

    async def receive_message(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        responses = []
        stub = self.get_stub(self, metadata)
        pass

    async def ack_message(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.AckMessage(request, deadline=duration)

    async def change_invisible_duration(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.ChangeInvisibleDuration(request, deadline=duration)

    async def forward_message_to_dead_letter_queue(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.ForwardMessageToDeadLetterQueue(request, deadline=duration)
    
    async def endTransaction(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.EndTransaction(request, deadline=duration)
    

    async def notifyClientTermination(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.NotifyClientTermination(request, deadline=duration)

    async def telemetry(self, metadata, duration, response_observer):
        stub = self.get_stub(self, metadata)
        return await stub.Telemetry(response_observer, deadline=duration)