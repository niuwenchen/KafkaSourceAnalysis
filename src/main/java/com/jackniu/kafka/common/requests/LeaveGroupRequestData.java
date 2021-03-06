package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiMessage;
import com.jackniu.kafka.common.protocol.MessageUtil;
import com.jackniu.kafka.common.protocol.Writable;
import com.jackniu.kafka.common.protocol.Readable;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.protocol.types.Type;

public class LeaveGroupRequestData  implements ApiMessage {
    private String groupId;
    private String memberId;

    public static final Schema SCHEMA_0 =
            new Schema(
                    new Field("group_id", Type.STRING, "The ID of the group to leave."),
                    new Field("member_id", Type.STRING, "The member ID to remove from the group.")
            );

    public static final Schema SCHEMA_1 = SCHEMA_0;

    public static final Schema SCHEMA_2 = SCHEMA_1;

    public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
    };

    public LeaveGroupRequestData(Readable readable, short version) {
        read(readable, version);
    }

    public LeaveGroupRequestData(Struct struct, short version) {
        fromStruct(struct, version);
    }

    public LeaveGroupRequestData() {
        this.groupId = "";
        this.memberId = "";
    }

    @Override
    public short apiKey() {
        return 13;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 2;
    }

    @Override
    public void read(Readable readable, short version) {
        this.groupId = readable.readNullableString();
        this.memberId = readable.readNullableString();
    }

    @Override
    public void write(Writable writable, short version) {
        writable.writeString(groupId);
        writable.writeString(memberId);
    }

    @Override
    public void fromStruct(Struct struct, short version) {
        this.groupId = struct.getString("group_id");
        this.memberId = struct.getString("member_id");
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(SCHEMAS[version]);
        struct.set("group_id", this.groupId);
        struct.set("member_id", this.memberId);
        return struct;
    }

    @Override
    public int size(short version) {
        int size = 0;
        size += 2;
        size += MessageUtil.serializedUtf8Length(groupId);
        size += 2;
        size += MessageUtil.serializedUtf8Length(memberId);
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LeaveGroupRequestData)) return false;
        LeaveGroupRequestData other = (LeaveGroupRequestData) obj;
        if (this.groupId == null) {
            if (other.groupId != null) return false;
        } else {
            if (!this.groupId.equals(other.groupId)) return false;
        }
        if (this.memberId == null) {
            if (other.memberId != null) return false;
        } else {
            if (!this.memberId.equals(other.memberId)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (groupId == null ? 0 : groupId.hashCode());
        hashCode = 31 * hashCode + (memberId == null ? 0 : memberId.hashCode());
        return hashCode;
    }

    @Override
    public String toString() {
        return "LeaveGroupRequestData("
                + "groupId='" + groupId + "'"
                + ", memberId='" + memberId + "'"
                + ")";
    }

    public String groupId() {
        return this.groupId;
    }

    public String memberId() {
        return this.memberId;
    }

    public LeaveGroupRequestData setGroupId(String v) {
        this.groupId = v;
        return this;
    }

    public LeaveGroupRequestData setMemberId(String v) {
        this.memberId = v;
        return this;
    }
}

