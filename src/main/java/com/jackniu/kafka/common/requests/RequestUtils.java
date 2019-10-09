package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.acl.AccessControlEntry;
import com.jackniu.kafka.common.acl.AccessControlEntryFilter;
import com.jackniu.kafka.common.acl.AclOperation;
import com.jackniu.kafka.common.acl.AclPermissionType;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.record.RecordBatch;
import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourcePattern;
import com.jackniu.kafka.common.resource.ResourcePatternFilter;
import com.jackniu.kafka.common.resource.ResourceType;

import java.util.Optional;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

final class RequestUtils {
    private RequestUtils() {}

    static ResourcePattern resourcePatternromStructFields(Struct struct) {
        byte resourceType = struct.get(RESOURCE_TYPE);
        String name = struct.get(RESOURCE_NAME);
        PatternType patternType = PatternType.fromCode(
                struct.getOrElse(RESOURCE_PATTERN_TYPE, PatternType.LITERAL.code()));
        return new ResourcePattern(ResourceType.fromCode(resourceType), name, patternType);
    }


    static void resourcePatternSetStructFields(ResourcePattern pattern, Struct struct) {
        struct.set(RESOURCE_TYPE, pattern.resourceType().code());
        struct.set(RESOURCE_NAME, pattern.name());
        struct.setIfExists(RESOURCE_PATTERN_TYPE, pattern.patternType().code());
    }

    static ResourcePatternFilter resourcePatternFilterFromStructFields(Struct struct) {
        byte resourceType = struct.get(RESOURCE_TYPE);
        String name = struct.get(RESOURCE_NAME_FILTER);
        PatternType patternType = PatternType.fromCode(
                struct.getOrElse(RESOURCE_PATTERN_TYPE_FILTER, PatternType.LITERAL.code()));
        return new ResourcePatternFilter(ResourceType.fromCode(resourceType), name, patternType);
    }

    static void resourcePatternFilterSetStructFields(ResourcePatternFilter patternFilter, Struct struct) {
        struct.set(RESOURCE_TYPE, patternFilter.resourceType().code());
        struct.set(RESOURCE_NAME_FILTER, patternFilter.name());
        struct.setIfExists(RESOURCE_PATTERN_TYPE_FILTER, patternFilter.patternType().code());
    }

    static AccessControlEntry aceFromStructFields(Struct struct) {
        String principal = struct.get(PRINCIPAL);
        String host = struct.get(HOST);
        byte operation = struct.get(OPERATION);
        byte permissionType = struct.get(PERMISSION_TYPE);
        return new AccessControlEntry(principal, host, AclOperation.fromCode(operation),
                AclPermissionType.fromCode(permissionType));
    }

    static void aceSetStructFields(AccessControlEntry data, Struct struct) {
        struct.set(PRINCIPAL, data.principal());
        struct.set(HOST, data.host());
        struct.set(OPERATION, data.operation().code());
        struct.set(PERMISSION_TYPE, data.permissionType().code());
    }

    static AccessControlEntryFilter aceFilterFromStructFields(Struct struct) {
        String principal = struct.get(PRINCIPAL_FILTER);
        String host = struct.get(HOST_FILTER);
        byte operation = struct.get(OPERATION);
        byte permissionType = struct.get(PERMISSION_TYPE);
        return new AccessControlEntryFilter(principal, host, AclOperation.fromCode(operation),
                AclPermissionType.fromCode(permissionType));
    }

    static void aceFilterSetStructFields(AccessControlEntryFilter filter, Struct struct) {
        struct.set(PRINCIPAL_FILTER, filter.principal());
        struct.set(HOST_FILTER, filter.host());
        struct.set(OPERATION, filter.operation().code());
        struct.set(PERMISSION_TYPE, filter.permissionType().code());
    }

    static void setLeaderEpochIfExists(Struct struct, Field.Int32 leaderEpochField, Optional<Integer> leaderEpoch) {
        struct.setIfExists(leaderEpochField, leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH));
    }

    static Optional<Integer> getLeaderEpoch(Struct struct, Field.Int32 leaderEpochField) {
        int leaderEpoch = struct.getOrElse(leaderEpochField, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        Optional<Integer> leaderEpochOpt = leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
        return leaderEpochOpt;
    }

}
