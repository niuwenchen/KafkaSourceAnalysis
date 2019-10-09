package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.header.Header;

import java.nio.ByteBuffer;

/**
 * A log record is a tuple consisting of a unique offset in the log, a sequence number assigned by
 * the producer, a timestamp, a key and a value.
 */
public interface Record  {
    Header[] EMPTY_HEADERS = new Header[0];

    /**
     * The offset of this record in the log
     * @return the offset
     */
    long offset();

    /**
     * Get the sequence number assigned by the producer.
     * @return the sequence number
     */
    int sequence();

    /**
     * Get the size in bytes of this record.
     * @return the size of the record in bytes
     */
    int sizeInBytes();

    /**
     * Get the record's timestamp.
     * @return the record's timestamp
     */
    long timestamp();

    /**
     * Get a checksum of the record contents.
     * @return A 4-byte unsigned checksum represented as a long or null if the message format does not
     *         include a checksum (i.e. for v2 and above)
     */
    Long checksumOrNull();

    /**
     * Check whether the record has a valid checksum.
     * @return true if the record has a valid checksum, false otherwise
     */
    boolean isValid();

    /**
     * Raise a {@link} if the record does not have a valid checksum.
     */
    void ensureValid();

    /**
     * Get the size in bytes of the key.
     * @return the size of the key, or -1 if there is no key
     */
    int keySize();

    /**
     * Check whether this record has a key
     * @return true if there is a key, false otherwise
     */
    boolean hasKey();

    /**
     * Get the record's key.
     * @return the key or null if there is none
     */
    ByteBuffer key();

    /**
     * Get the size in bytes of the value.
     * @return the size of the value, or -1 if the value is null
     */
    int valueSize();

    /**
     * Check whether a value is present (i.e. if the value is not null)
     * @return true if so, false otherwise
     */
    boolean hasValue();

    /**
     * Get the record's value
     * @return the (nullable) value
     */
    ByteBuffer value();

    /**
     * Check whether the record has a particular magic. For versions prior to 2, the record contains its own magic,
     * so this function can be used to check whether it matches a particular value. For version 2 and above, this
     * method returns true if the passed magic is greater than or equal to 2.
     *
     * @param magic the magic value to check
     * @return true if the record has a magic field (versions prior to 2) and the value matches
     */
    boolean hasMagic(byte magic);

    /**
     * For versions prior to 2, check whether the record is compressed (and therefore
     * has nested record content). For versions 2 and above, this always returns false.
     * @return true if the magic is lower than 2 and the record is compressed
     */
    boolean isCompressed();

    /**
     * For versions prior to 2, the record contained a timestamp type attribute. This method can be
     * used to check whether the value of that attribute matches a particular timestamp type. For versions
     * 2 and above, this will always be false.
     *
     * @param timestampType the timestamp type to compare
     * @return true if the version is lower than 2 and the timestamp type matches
     */
    boolean hasTimestampType(TimestampType timestampType);

    /**
     * Get the headers. For magic versions 1 and below, this always returns an empty array.
     *
     * @return the array of headers
     */
    Header[] headers();
}