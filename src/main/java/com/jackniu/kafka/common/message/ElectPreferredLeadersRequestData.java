package com.jackniu.kafka.common.message;

import com.jackniu.kafka.common.protocol.ApiMessage;
import com.jackniu.kafka.common.protocol.Message;
import com.jackniu.kafka.common.protocol.MessageUtil;
import com.jackniu.kafka.common.protocol.Writable;
import com.jackniu.kafka.common.protocol.Readable;
import com.jackniu.kafka.common.protocol.types.*;

import java.util.ArrayList;
import java.util.List;

public class ElectPreferredLeadersRequestData   implements ApiMessage {
    private List<TopicPartitions> topicPartitions;
    private int timeoutMs;

    public static final Schema SCHEMA_0 =
            new Schema(
                    new Field("topic_partitions", ArrayOf.nullable(TopicPartitions.SCHEMA_0), "The topic partitions to elect the preferred leader of."),
                    new Field("timeout_ms", Type.INT32, "The time in ms to wait for the election to complete.")
            );

    public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
    };

    public ElectPreferredLeadersRequestData(Readable readable, short version) {
        this.topicPartitions = new ArrayList<TopicPartitions>();
        read(readable, version);
    }

    public ElectPreferredLeadersRequestData(Struct struct, short version) {
        this.topicPartitions = new ArrayList<TopicPartitions>();
        fromStruct(struct, version);
    }

    public ElectPreferredLeadersRequestData() {
        this.topicPartitions = new ArrayList<TopicPartitions>();
        this.timeoutMs = 60000;
    }

    @Override
    public short apiKey() {
        return 43;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 0;
    }

    @Override
    public void read(Readable readable, short version) {
        {
            int arrayLength = readable.readInt();
            if (arrayLength < 0) {
                this.topicPartitions.clear();
            } else {
                this.topicPartitions.clear();
                for (int i = 0; i < arrayLength; i++) {
                    this.topicPartitions.add(new TopicPartitions(readable, version));
                }
            }
        }
        this.timeoutMs = readable.readInt();
    }

    @Override
    public void write(Writable writable, short version) {
        if (topicPartitions == null) {
            writable.writeInt(-1);
        } else {
            writable.writeInt(topicPartitions.size());
            for (TopicPartitions element : topicPartitions) {
                element.write(writable, version);
            }
        }
        writable.writeInt(timeoutMs);
    }

    @Override
    public void fromStruct(Struct struct, short version) {
        {
            Object[] nestedObjects = struct.getArray("topic_partitions");
            if (nestedObjects == null) {
                this.topicPartitions = null;
            } else {
                this.topicPartitions = new ArrayList<TopicPartitions>(nestedObjects.length);
                for (Object nestedObject : nestedObjects) {
                    this.topicPartitions.add(new TopicPartitions((Struct) nestedObject, version));
                }
            }
        }
        this.timeoutMs = struct.getInt("timeout_ms");
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(SCHEMAS[version]);
        {
            if (topicPartitions == null) {
                struct.set("topic_partitions", null);
            } else {
                Struct[] nestedObjects = new Struct[topicPartitions.size()];
                int i = 0;
                for (TopicPartitions element : this.topicPartitions) {
                    nestedObjects[i++] = element.toStruct(version);
                }
                struct.set("topic_partitions", (Object[]) nestedObjects);
            }
        }
        struct.set("timeout_ms", this.timeoutMs);
        return struct;
    }

    @Override
    public int size(short version) {
        int size = 0;
        if (topicPartitions == null) {
            size += 4;
        } else {
            size += 4;
            for (TopicPartitions element : topicPartitions) {
                size += element.size(version);
            }
        }
        size += 4;
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ElectPreferredLeadersRequestData)) return false;
        ElectPreferredLeadersRequestData other = (ElectPreferredLeadersRequestData) obj;
        if (this.topicPartitions == null) {
            if (other.topicPartitions != null) return false;
        } else {
            if (!this.topicPartitions.equals(other.topicPartitions)) return false;
        }
        if (timeoutMs != other.timeoutMs) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (topicPartitions == null ? 0 : topicPartitions.hashCode());
        hashCode = 31 * hashCode + timeoutMs;
        return hashCode;
    }

    @Override
    public String toString() {
        return "ElectPreferredLeadersRequestData("
                + "topicPartitions=" + MessageUtil.deepToString(topicPartitions.iterator())
                + ", timeoutMs=" + timeoutMs
                + ")";
    }

    public List<TopicPartitions> topicPartitions() {
        return this.topicPartitions;
    }

    public int timeoutMs() {
        return this.timeoutMs;
    }

    public ElectPreferredLeadersRequestData setTopicPartitions(List<TopicPartitions> v) {
        this.topicPartitions = v;
        return this;
    }

    public ElectPreferredLeadersRequestData setTimeoutMs(int v) {
        this.timeoutMs = v;
        return this;
    }

    static public class TopicPartitions implements Message {
        private String topic;
        private List<Integer> partitionId;

        public static final Schema SCHEMA_0 =
                new Schema(
                        new Field("topic", Type.STRING, "The name of a topic."),
                        new Field("partition_id", new ArrayOf(Type.INT32), "The partitions of this topic whose preferred leader should be elected")
                );

        public static final Schema[] SCHEMAS = new Schema[] {
                SCHEMA_0
        };

        public TopicPartitions(Readable readable, short version) {
            this.partitionId = new ArrayList<Integer>();
            read(readable, version);
        }

        public TopicPartitions(Struct struct, short version) {
            this.partitionId = new ArrayList<Integer>();
            fromStruct(struct, version);
        }

        public TopicPartitions() {
            this.topic = "";
            this.partitionId = new ArrayList<Integer>();
        }


        @Override
        public short lowestSupportedVersion() {
            return 0;
        }

        @Override
        public short highestSupportedVersion() {
            return 0;
        }

        @Override
        public void read(Readable readable, short version) {
            this.topic = readable.readNullableString();
            {
                int arrayLength = readable.readInt();
                if (arrayLength < 0) {
                    this.partitionId.clear();
                } else {
                    this.partitionId.clear();
                    for (int i = 0; i < arrayLength; i++) {
                        this.partitionId.add(readable.readInt());
                    }
                }
            }
        }

        @Override
        public void write(Writable writable, short version) {
            writable.writeString(topic);
            writable.writeInt(partitionId.size());
            for (Integer element : partitionId) {
                writable.writeInt(element);
            }
        }

        @Override
        public void fromStruct(Struct struct, short version) {
            this.topic = struct.getString("topic");
            {
                Object[] nestedObjects = struct.getArray("partition_id");
                this.partitionId = new ArrayList<Integer>(nestedObjects.length);
                for (Object nestedObject : nestedObjects) {
                    this.partitionId.add((Integer) nestedObject);
                }
            }
        }

        @Override
        public Struct toStruct(short version) {
            Struct struct = new Struct(SCHEMAS[version]);
            struct.set("topic", this.topic);
            {
                Integer[] nestedObjects = new Integer[partitionId.size()];
                int i = 0;
                for (Integer element : this.partitionId) {
                    nestedObjects[i++] = element;
                }
                struct.set("partition_id", (Object[]) nestedObjects);
            }
            return struct;
        }

        @Override
        public int size(short version) {
            int size = 0;
            size += 2;
            size += MessageUtil.serializedUtf8Length(topic);
            size += 4;
            size += partitionId.size() * 4;
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicPartitions)) return false;
            TopicPartitions other = (TopicPartitions) obj;
            if (this.topic == null) {
                if (other.topic != null) return false;
            } else {
                if (!this.topic.equals(other.topic)) return false;
            }
            if (this.partitionId == null) {
                if (other.partitionId != null) return false;
            } else {
                if (!this.partitionId.equals(other.partitionId)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
            hashCode = 31 * hashCode + (partitionId == null ? 0 : partitionId.hashCode());
            return hashCode;
        }

        @Override
        public String toString() {
            return "TopicPartitions("
                    + "topic='" + topic + "'"
                    + ", partitionId=" + MessageUtil.deepToString(partitionId.iterator())
                    + ")";
        }

        public String topic() {
            return this.topic;
        }

        public List<Integer> partitionId() {
            return this.partitionId;
        }

        public TopicPartitions setTopic(String v) {
            this.topic = v;
            return this;
        }

        public TopicPartitions setPartitionId(List<Integer> v) {
            this.partitionId = v;
            return this;
        }
    }
}

