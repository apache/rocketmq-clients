package org.apache.rocketmq.client.producer;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

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

    public void onReceiveResponse(SendMessageResponse response) {
        try {
            checkNotNull(response);
            final ServiceState state = this.state.get();
            if (ServiceState.STARTED != state) {
                log.info("Client is not be started, state={}", state);
                return;
            }
            SendResult sendResult;
            try {
                sendResult = ClientInstance.processSendResponse(response);
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
