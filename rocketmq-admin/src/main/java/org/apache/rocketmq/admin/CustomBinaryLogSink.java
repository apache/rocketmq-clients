package org.apache.rocketmq.admin;

import com.google.protobuf.MessageLite;
import io.grpc.services.BinaryLogSink;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomBinaryLogSink implements BinaryLogSink {
    private final String outPath;
    private final OutputStream out;
    private boolean closed;

    public CustomBinaryLogSink(String path) throws IOException {
        File outFile = new File(path);
        outPath = outFile.getAbsolutePath();
        log.info("Writing binary logs to {}", outFile.getAbsolutePath());
        out = new BufferedOutputStream(new FileOutputStream(outFile));
    }

    String getPath() {
        return this.outPath;
    }

    @Override
    public synchronized void write(MessageLite message) {
        if (closed) {
            log.info("Attempt to write after TempFileSink is closed.");
            return;
        }
        try {
            out.write(message.toString().getBytes("UTF-8"));
            out.write("-------------------------\n".getBytes("UTF-8"));
            out.flush();
        } catch (IOException e) {
            log.info("Caught exception while writing", e);
            closeQuietly();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.flush();
        } finally {
            out.close();
        }
    }

    private synchronized void closeQuietly() {
        try {
            close();
        } catch (IOException e) {
            log.info("Caught exception while closing", e);
        }
    }
}
