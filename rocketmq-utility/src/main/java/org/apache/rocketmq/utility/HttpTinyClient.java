/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.utility;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;

public class HttpTinyClient {
    private static final String GET_METHOD = "GET";

    private static final HttpTinyClient INSTANCE = new HttpTinyClient();

    private HttpTinyClient() {
    }

    public static HttpTinyClient getInstance() {
        return INSTANCE;
    }

    public ListenableFuture<HttpResult> httpGet(final String url, final int timeoutMillis, ExecutorService executor) {
        final SettableFuture<HttpResult> future0 = SettableFuture.create();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final HttpResult httpResult = httpGet(url, timeoutMillis);
                        future0.set(httpResult);
                    } catch (Throwable t) {
                        future0.setException(t);
                    }
                }
            });
        } catch (Throwable t) {
            future0.setException(t);
        }
        return future0;
    }

    public HttpResult httpGet(String url, int timeoutMillis) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod(GET_METHOD);
            conn.setConnectTimeout(timeoutMillis);
            conn.setReadTimeout(timeoutMillis);

            conn.connect();
            int respCode = conn.getResponseCode();
            String resp = HttpURLConnection.HTTP_OK == respCode ? toString(conn.getInputStream(),
                                                                           UtilAll.DEFAULT_CHARSET) :
                          toString(conn.getErrorStream(), UtilAll.DEFAULT_CHARSET);
            return new HttpResult(respCode, resp);
        } finally {
            if (null != conn) {
                conn.disconnect();
            }
        }
    }

    public static class HttpResult {
        private final int code;
        private final String content;

        public HttpResult(int code, String content) {
            this.code = code;
            this.content = content;
        }

        public boolean isOk() {
            return HttpURLConnection.HTTP_OK == code;
        }

        public int getCode() {
            return this.code;
        }

        public String getContent() {
            return this.content;
        }
    }

    public static String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, UtilAll.DEFAULT_CHARSET)) :
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
