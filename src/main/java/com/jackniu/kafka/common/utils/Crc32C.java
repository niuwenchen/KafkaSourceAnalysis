package com.jackniu.kafka.common.utils;


import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;



// A class that can be used to compute the CRC32C (Castagnoli) of a ByteBuffer or array of bytes.
public final class Crc32C {

    private static final ChecksumFactory CHECKSUM_FACTORY;

    static {
        if (Java.IS_JAVA9_COMPATIBLE)
            CHECKSUM_FACTORY = new Java9ChecksumFactory();
        else
            CHECKSUM_FACTORY = new PureJavaChecksumFactory();
    }

    private Crc32C() {}

    public static long compute(byte[] bytes, int offset, int size) {
        Checksum crc = create();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }
    public static long compute(ByteBuffer buffer, int offset, int size) {
        Checksum crc = create();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }

    public static Checksum create() {
        return CHECKSUM_FACTORY.create();
    }

    private interface ChecksumFactory {
        Checksum create();
    }

    private static class Java9ChecksumFactory implements ChecksumFactory {
        private static final MethodHandle CONSTRUCTOR;

        static {
            try {
                Class<?> cls = Class.forName("java.util.zip.CRC32C");
                CONSTRUCTOR = MethodHandles.publicLookup().findConstructor(cls, MethodType.methodType(void.class));
            } catch (ReflectiveOperationException e) {
                // Should never happen
                throw new RuntimeException(e);
            }
        }

        @Override
        public Checksum create() {
            try {
                return (Checksum) CONSTRUCTOR.invoke();
            } catch (Throwable throwable) {
                // Should never happen
                throw new RuntimeException(throwable);
            }
        }
    }

    private static class PureJavaChecksumFactory implements ChecksumFactory {
        @Override
        public Checksum create() {
            return new PureJavaCrc32C();
        }
    }

}
