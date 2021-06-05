package org.apache.rocketmq.client.remoting;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.rocketmq.client.misc.MixAll;

public class TlsHelper {

    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    private TlsHelper() {
    }

    public static String sign(String accessSecret, String dateTime) throws UnsupportedEncodingException,
                                                                           NoSuchAlgorithmException,
                                                                           InvalidKeyException {
        SecretKeySpec signingKey = new SecretKeySpec(accessSecret.getBytes(MixAll.DEFAULT_CHARSET),
                                                     HMAC_SHA1_ALGORITHM);
        Mac mac;
        mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
        mac.init(signingKey);

        return Hex.encodeHexString(mac.doFinal(dateTime.getBytes(MixAll.DEFAULT_CHARSET)), false);
    }
}
