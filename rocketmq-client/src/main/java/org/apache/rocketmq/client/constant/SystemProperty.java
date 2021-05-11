package org.apache.rocketmq.client.constant;

public class SystemProperty {
    public static final String NAME_SERVER_ADDR = "rocketmq.namesrv.addr";
    public static final String NAME_SERVER_DOMAIN = "rocketmq.namesrv.domain";
    public static final String NAME_SERVER_SUB_GROUP = "rocketmq.namesrv.domain.subgroup";

    public static final String MESSAGE_COMPRESSION_LEVEL = "rocketmq.message.compressLevel";

    public static final String TLS_SERVER_MODE = "tls.server.mode";
    public static final String TLS_ENABLE = "tls.enable";
    public static final String TLS_CONFIG_FILE = "tls.config.file";
    public static final String TLS_TEST_MODE_ENABLE = "tls.test.mode.enable";
    public static final String TLS_SERVER_NEED_CLIENT_AUTH = "tls.server.need.client.auth";
    public static final String TLS_SERVER_KEY_PATH = "tls.server.keyPath";
    public static final String TLS_SERVER_KEY_PASSWORD = "tls.server.keyPassword";
    public static final String TLS_SERVER_CERT_PATH = "tls.server.certPath";
    public static final String TLS_SERVER_AUTH_CLIENT = "tls.server.authClient";
    public static final String TLS_SERVER_TRUST_CERT_PATH = "tls.server.trustCertPath";
    public static final String TLS_CLIENT_KEY_PATH = "tls.client.keyPath";
    public static final String TLS_CLIENT_KEY_PASSWORD = "tls.client.keyPassword";
    public static final String TLS_CLIENT_CERT_PATH = "tls.client.certPath";
    public static final String TLS_CLIENT_AUTH_SERVER = "tls.client.authServer";
    public static final String TLS_CLIENT_TRUST_CERT_PATH = "tls.client.trustCertPath";

    private SystemProperty() {
    }
}
