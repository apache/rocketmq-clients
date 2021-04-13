package org.apache.rocketmq.client.misc;

import org.apache.rocketmq.client.constant.SystemProperty;
import org.apache.rocketmq.client.constant.TLSMode;

public class TLSSystemConfig {

  /** To determine whether use SSL in client-side, include SDK client and BrokerOuterAPI */
  public static boolean tlsEnable =
      Boolean.parseBoolean(System.getProperty(SystemProperty.TLS_ENABLE, "false"));

  /** To determine whether use test mode when initialize TLS context */
  public static boolean tlsTestModeEnable =
      Boolean.parseBoolean(System.getProperty(SystemProperty.TLS_TEST_MODE_ENABLE, "true"));

  /**
   * Indicates the state of the {@link javax.net.ssl.SSLEngine} with respect to client
   * authentication. This configuration item really only applies when building the server-side, and
   * can be set to none, require or optional.
   */
  public static String tlsServerNeedClientAuth =
      System.getProperty(SystemProperty.TLS_SERVER_NEED_CLIENT_AUTH, "none");
  /** The store path of server-side private key */
  public static String tlsServerKeyPath =
      System.getProperty(SystemProperty.TLS_SERVER_KEY_PATH, null);

  /** The password of the server-side private key */
  public static String tlsServerKeyPassword =
      System.getProperty(SystemProperty.TLS_SERVER_KEY_PASSWORD, null);

  /** The store path of server-side X.509 certificate chain in PEM format */
  public static String tlsServerCertPath =
      System.getProperty(SystemProperty.TLS_SERVER_CERT_PATH, null);

  /** To determine whether verify the client endpoint's certificate strictly */
  public static boolean tlsServerAuthClient =
      Boolean.parseBoolean(System.getProperty(SystemProperty.TLS_SERVER_AUTH_CLIENT, "false"));

  /** The store path of trusted certificates for verifying the client endpoint's certificate */
  public static String tlsServerTrustCertPath =
      System.getProperty(SystemProperty.TLS_SERVER_TRUST_CERT_PATH, null);

  /** The store path of client-side private key */
  public static String tlsClientKeyPath =
      System.getProperty(SystemProperty.TLS_CLIENT_KEY_PATH, null);

  /** The password of the client-side private key */
  public static String tlsClientKeyPassword =
      System.getProperty(SystemProperty.TLS_CLIENT_KEY_PASSWORD, null);

  /** The store path of client-side X.509 certificate chain in PEM format */
  public static String tlsClientCertPath =
      System.getProperty(SystemProperty.TLS_CLIENT_CERT_PATH, null);

  /** To determine whether verify the server endpoint's certificate strictly */
  public static boolean tlsClientAuthServer =
      Boolean.parseBoolean(System.getProperty(SystemProperty.TLS_CLIENT_AUTH_SERVER, "false"));

  /** The store path of trusted certificates for verifying the server endpoint's certificate */
  public static String tlsClientTrustCertPath =
      System.getProperty(SystemProperty.TLS_CLIENT_TRUST_CERT_PATH, null);

  /**
   * For server, three SSL modes are supported: disabled, permissive and enforcing. For client, use
   * {@link TLSSystemConfig#tlsEnable} to determine whether use SSL.
   */
  public static TLSMode tlsMode =
      TLSMode.parse(
          System.getProperty(SystemProperty.TLS_SERVER_MODE, TLSMode.PERMISSIVE.getMode()));

  /**
   * A config file to store the above TLS related configurations, except {@link
   * TLSSystemConfig#tlsMode} and {@link TLSSystemConfig#tlsEnable}
   */
  public static String tlsConfigFile =
      System.getProperty(SystemProperty.TLS_CONFIG_FILE, "/etc/rocketmq" + "/tls.properties");
}
