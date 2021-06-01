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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.testng.annotations.Test;

public class RPCClientImplTest {

    @Test
    public void testQueryRoute() {
        final RPCClientImpl rpcClient = new RPCClientImpl(new RpcTarget("11.165.223.199:9876"));

        Resource topicResource = Resource.newBuilder().setName("yc001").build();

        QueryRouteRequest request =
                QueryRouteRequest.newBuilder().setTopic(topicResource).build();
        final QueryRouteResponse response = rpcClient.queryRoute(request, 3, TimeUnit.SECONDS);
        System.out.println(response);
    }

    @Test
    public void testSendMessage() throws UnsupportedEncodingException {
        final RPCClientImpl rpcClient = new RPCClientImpl(new RpcTarget("11.158.159.57:8081"));
        rpcClient.setAccessCredential(new AccessCredential("LTAInDOvOPEkCj67", "UniBnf6GKgUS1Y5l3Ce0rmgQhhKyZd"));

        rpcClient.setArn("MQ_INST_1973281269661160_BXmPlOA6");

        final Resource topicResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "yc001").build();
        SystemAttribute systemAttribute = SystemAttribute.newBuilder().setMessageId("sdfsdfsdf").build();
        Message msg =
                Message.newBuilder().setTopic(topicResource).setSystemAttribute(systemAttribute).setBody(ByteString.copyFrom(
                        "Hello", "UTF-8")).build();

        SendMessageRequest request =
                SendMessageRequest.newBuilder().setMessage(msg).build();

        final SendMessageResponse response = rpcClient.sendMessage(request, 3, TimeUnit.SECONDS);
        System.out.println(response);
    }

    @Test
    public void testQueryAssignment() {
        final RPCClientImpl rpcClient = new RPCClientImpl(new RpcTarget("11.158.159.57:8081"));
        rpcClient.setAccessCredential(new AccessCredential("LTAInDOvOPEkCj67", "UniBnf6GKgUS1Y5l3Ce0rmgQhhKyZd"));
        rpcClient.setTenantId("");
        rpcClient.setArn("MQ_INST_1973281269661160_BXmPlOA6");


        final Resource topicResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "yc001").build();
        final Resource groupResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "GID_groupa").build();


        QueryAssignmentRequest request =
                QueryAssignmentRequest.newBuilder().setClientId("123").setTopic(topicResource).setGroup(groupResource).setConsumeModel(ConsumeModel.CLUSTERING).build();
        final QueryAssignmentResponse response = rpcClient.queryAssignment(request, 3, TimeUnit.SECONDS);
        System.out.println(response);
    }

}