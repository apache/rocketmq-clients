package org.apache.rocketmq.client.remoting;

import com.google.common.base.Objects;
import lombok.Getter;

@Getter
public class RpcTarget {

    private final String target;
    private volatile boolean isolated;

    public RpcTarget(String target) {
        this.target = target;
        this.isolated = false;
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RpcTarget that = (RpcTarget) o;
        return Objects.equal(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(target);
    }
}
