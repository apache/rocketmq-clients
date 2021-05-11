package org.apache.rocketmq.client.remoting;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicReference;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;

@Slf4j
@Setter
public class SendMessageResponseCallback {
    private SendMessageRequest request;
    private AtomicReference<ServiceState> state;
    private SendCallback sendCallback;

    public SendMessageResponseCallback(
            SendMessageRequest request, AtomicReference<ServiceState> state, SendCallback sendCallback) {
        this.request = request;
        this.state = state;
        this.sendCallback = sendCallback;
    }

    public void onSuccess(SendMessageResponse response) {
        try {
            checkNotNull(response);
            final ServiceState state = this.state.get();
            if (ServiceState.STARTED != state) {
                log.info("Client is not be started, state={}", state);
                return;
            }
            MessageQueue messageQueue =
                    new MessageQueue(
                            request.getMessage().getTopic(), request.getBrokerName(), response.getQueueId());
            SendResult sendResult;
            try {
                sendResult = ClientInstance.processSendResponse(messageQueue, response);
            } catch (MQServerException e) {
                sendCallback.onException(e);
                return;
            }
            sendCallback.onSuccess(sendResult);
        } catch (Throwable t) {
            log.error("Unexpected error while invoking user-defined callback.", t);
        }
    }

    public void onException(final Throwable e) {
        try {
            checkNotNull(e);
            sendCallback.onException(e);
        } catch (Throwable t) {
            log.error("Unexpected error while invoking user-defined callback.", t);
        }
    }
}
