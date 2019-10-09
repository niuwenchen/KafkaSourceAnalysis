package com.jackniu.kafka.common.protocol;

import com.jackniu.kafka.common.utils.Utils;

import java.util.Iterator;

public final  class MessageUtil {
    /**
     * Get the length of the UTF8 representation of a string, without allocating
     * a byte buffer for the string.
     */
    public static short serializedUtf8Length(CharSequence input) {
        int count = Utils.utf8Length(input);
        if (count > Short.MAX_VALUE) {
            throw new RuntimeException("String " + input + " is too long to serialize.");
        }
        return (short) count;
    }

    public static String deepToString(Iterator<?> iter) {
        StringBuilder bld = new StringBuilder("[");
        String prefix = "";
        while (iter.hasNext()) {
            Object object = iter.next();
            bld.append(prefix);
            bld.append(object.toString());
            prefix = ", ";
        }
        bld.append("]");
        return bld.toString();
    }
}

