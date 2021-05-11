package org.apache.rocketmq.utility;

public class PermissionHelper {
    public static final int PERM_INHERIT = 0x1;
    public static final int PERM_WRITE = 0x1 << 1;
    public static final int PERM_READ = 0x1 << 2;

    private PermissionHelper() {
    }

    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }

    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }

    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }
}
