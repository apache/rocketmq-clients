package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SystemProperty {
  NAMESRV_ADDR_PROPERTY("rocketmq.namesrv.addr"),

  TLS_SERVER_MODE("tls.server.mode"),
  TLS_ENABLE("tls.enable"),
  TLS_CONFIG_FILE("tls.config.file"),
  TLS_TEST_MODE_ENABLE("tls.test.mode.enable"),

  TLS_SERVER_NEED_CLIENT_AUTH("tls.server.need.client.auth"),
  TLS_SERVER_KEY_PATH("tls.server.keyPath"),
  TLS_SERVER_KEY_PASSWORD("tls.server.keyPassword"),
  TLS_SERVER_CERT_PATH("tls.server.certPath"),
  TLS_SERVER_AUTH_CLIENT("tls.server.authClient"),
  TLS_SERVER_TRUST_CERT_PATH("tls.server.trustCertPath"),

  TLS_CLIENT_KEY_PATH("tls.client.keyPath"),
  TLS_CLIENT_KEY_PASSWORD("tls.client.keyPassword"),
  TLS_CLIENT_CERT_PATH("tls.client.certPath"),
  TLS_CLIENT_AUTH_SERVER("tls.client.authServer"),
  TLS_CLIENT_TRUST_CERT_PATH("tls.client.trustCertPath");

  private final String name;
}
