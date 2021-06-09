package org.apache.rocketmq.client.producer;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.SendMessageResponse;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.remoting.RpcTarget;

@Slf4j
@Setter
public class SendMessageResponseCallback {
    private final RpcTarget rpcTarget;
    private final AtomicReference<ServiceState> state;
    private final SendCallback sendCallback;

    public SendMessageResponseCallback(RpcTarget rpcTarget,
                                       AtomicReference<ServiceState> state,
                                       SendCallback sendCallback) {
        this.rpcTarget = rpcTarget;
        this.state = state;
        this.sendCallback = sendCallback;
    }

    public void onReceiveResponse(SendMessageResponse response) {
        try {
            final ServiceState state = this.state.get();
            if (ServiceState.STARTED != state) {
                log.info("Client is not be started, state={}", state);
                return;
            }
            SendResult sendResult;
            try {
                sendResult = ClientInstance.processSendResponse(rpcTarget, response);
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
