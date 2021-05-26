package org.apache.rocketmq.client.remoting;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;

public class TlsHelper {

    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    private TlsHelper() {
    }

    public static String sign(String accessSecret, String dateTime) {
        SecretKeySpec signingKey = new SecretKeySpec(accessSecret.getBytes(), HMAC_SHA1_ALGORITHM);
        Mac mac;
        try {
            mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
            mac.init(signingKey);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("HmacSHA1 algorithm is not supported.");
        } catch (InvalidKeyException e) {
            throw new RuntimeException("SigningKey is invalid unexpectedly.");
        }
        return Hex.encodeHexString(mac.doFinal(dateTime.getBytes()), false);
    }
}
