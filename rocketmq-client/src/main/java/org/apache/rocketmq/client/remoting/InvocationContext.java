package org.apache.rocketmq.client.remoting;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.function.BiConsumer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class InvocationContext<T> {
  private final BiConsumer<T, Throwable> callback;

  public InvocationContext(BiConsumer<T, Throwable> callback) {
    this.callback = callback;
  }

  public void onSuccess(final T response) {
    try {
      checkNotNull(response);
      callback.accept(response, null);
    } catch (Throwable t) {
      log.error("Unexpected error while invoking user-defined callback.", t);
    }
  }

  public void onException(final Throwable e) {
    try {
      checkNotNull(e);
      callback.accept(null, e);
    } catch (Throwable t) {
      log.error("Unexpected error while invoking user-defined callback.", t);
    }
  }
}
