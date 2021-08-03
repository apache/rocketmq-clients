package org.apache.rocketmq.utility;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class HttpTinyClient {
    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final String GET_METHOD = "GET";

    private HttpTinyClient() {
    }

    public static HttpResult httpGet(String url, int timeoutMillis) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod(GET_METHOD);
            conn.setConnectTimeout(timeoutMillis);
            conn.setReadTimeout(timeoutMillis);

            conn.connect();
            int respCode = conn.getResponseCode();
            String resp = HttpURLConnection.HTTP_OK == respCode ? toString(conn.getInputStream(), DEFAULT_CHARSET) :
                          toString(conn.getErrorStream(), DEFAULT_CHARSET);
            return new HttpResult(respCode, resp);
        } finally {
            if (null != conn) {
                conn.disconnect();
            }
        }
    }

    @AllArgsConstructor
    @Getter
    public static class HttpResult {
        public final int code;
        public final String content;

        public boolean isOk() {
            return HttpURLConnection.HTTP_OK == code;
        }
    }

    public static String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, DEFAULT_CHARSET)) :
               toString(new InputStreamReader(input, encoding));
    }

    public static String toString(Reader reader) throws IOException {
        CharArrayWriter sw = new CharArrayWriter();
        copy(reader, sw);
        return sw.toString();
    }

    public static void copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[1 << 12];
        int n;
        while ((n = input.read(buffer)) >= 0) {
            output.write(buffer, 0, n);
        }
    }
}
