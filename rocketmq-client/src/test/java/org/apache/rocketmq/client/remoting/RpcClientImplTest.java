package org.apache.rocketmq.client.remoting;

import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.route.AddressScheme;
import org.testng.annotations.Test;

public class RpcClientImplTest {

    private CredentialsObservable observable = new CredentialsObservable() {

        @Override
        public AccessCredential getAccessCredential() {
            return new AccessCredential("LTAInDOvOPEkCj67", "UniBnf6GKgUS1Y5l3Ce0rmgQhhKyZd");
        }

        @Override
        public String getTenantId() {
            return "";
        }

        @Override
        public String getArn() {
            return "MQ_INST_1973281269661160_BXmPlOA6";
        }

        @Override
        public String getRegionId() {
            return "";
        }

        @Override
        public String getServiceName() {
            return "";
        }
    };

    @Test
    public void testQueryRoute() throws SSLException {
        List<Address> addresses = new ArrayList<Address>();
        addresses.add(new Address("11.165.223.199", 9876));
        final Endpoints endpoints = new Endpoints(AddressScheme.IPv4, addresses);

        final RpcClientImpl rpcClient = new RpcClientImpl(new RpcTarget(endpoints, true, false), observable);

        Resource topicResource = Resource.newBuilder().setName("yc001").build();

        QueryRouteRequest request =
                QueryRouteRequest.newBuilder().setTopic(topicResource).build();
        final QueryRouteResponse response = rpcClient.queryRoute(request, 3, TimeUnit.SECONDS);
        System.out.println(response);
    }

    @Test
    public void testSendMessage() throws UnsupportedEncodingException, SSLException {

        List<Address> addresses = new ArrayList<Address>();
        addresses.add(new Address("11.158.159.57", 8081));
        final Endpoints endpoints = new Endpoints(AddressScheme.IPv4, addresses);
        final RpcClientImpl rpcClient = new RpcClientImpl(new RpcTarget(endpoints, false, true), observable);

        final Resource topicResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "yc001").build();
        SystemAttribute systemAttribute = SystemAttribute.newBuilder().setMessageId("sdfsdfsdf").build();
        Message msg =
                Message.newBuilder().setTopic(topicResource).setSystemAttribute(systemAttribute).setBody(ByteString.copyFrom(
                        "Hello", "UTF-8")).build();

        SendMessageRequest request =
                SendMessageRequest.newBuilder().setMessage(msg).build();

        final SendMessageResponse response = rpcClient.sendMessage(request, 3, TimeUnit.SECONDS);
        System.out.println(request);
    }

    @Test
    public void testQueryAssignment() throws SSLException {
        List<Address> addresses = new ArrayList<Address>();
        addresses.add(new Address("11.158.159.57", 8081));
        final Endpoints endpoints = new Endpoints(AddressScheme.IPv4, addresses);
        final RpcClientImpl rpcClient = new RpcClientImpl(new RpcTarget(endpoints, false, true), observable);

        final Resource topicResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "yc001").build();
        final Resource groupResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "GID_groupa").build();

        QueryAssignmentRequest request =
                QueryAssignmentRequest.newBuilder().setClientId("123").setTopic(topicResource).setGroup(groupResource).build();
        final QueryAssignmentResponse response = rpcClient.queryAssignment(request, 3, TimeUnit.SECONDS);
        System.out.println(response);
    }

}