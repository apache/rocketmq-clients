package org.apache.rocketmq.client.remoting;

public class StaticCredentialsProvider implements CredentialsProvider {
    private final Credentials credentials;

    public StaticCredentialsProvider(String accessKey, String accessSecret) {
        this.credentials = new Credentials(accessKey, accessSecret);
    }

    @Override
    public Credentials getCredentials() {
        return credentials;
    }
}
