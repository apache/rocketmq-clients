package org.apache.rocketmq.utility;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface CLibrary extends Library {
    CLibrary INSTANCE = Native.load("c", CLibrary.class);

    int getpid();
}
