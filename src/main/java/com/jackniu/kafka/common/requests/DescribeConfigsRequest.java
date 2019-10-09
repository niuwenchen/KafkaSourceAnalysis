package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.config.ConfigResource;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

import static com.jackniu.kafka.common.protocol.types.Type.*;

public class DescribeConfigsRequest  extends AbstractRequest {

    private static final String RESOURCES_KEY_NAME = "resources";
    private static final String INCLUDE_SYNONYMS = "include_synonyms";
    private static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    private static final String RESOURCE_NAME_KEY_NAME = "resource_name";
    private static final String CONFIG_NAMES_KEY_NAME = "config_names";

    private static final Schema DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0 = new Schema(
            new Field(RESOURCE_TYPE_KEY_NAME, INT8),
            new Field(RESOURCE_NAME_KEY_NAME, STRING),
            new Field(CONFIG_NAMES_KEY_NAME, ArrayOf.nullable(STRING)));

    private static final Schema DESCRIBE_CONFIGS_REQUEST_V0 = new Schema(
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0), "An array of config resources to be returned."));

    private static final Schema DESCRIBE_CONFIGS_REQUEST_V1 = new Schema(
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0), "An array of config resources to be returned."),
            new Field(INCLUDE_SYNONYMS, BOOLEAN));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_CONFIGS_REQUEST_V2 = DESCRIBE_CONFIGS_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_CONFIGS_REQUEST_V0, DESCRIBE_CONFIGS_REQUEST_V1, DESCRIBE_CONFIGS_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder {
        private final Map<ConfigResource, Collection<String>> resourceToConfigNames;
        private boolean includeSynonyms;

        public Builder(Map<ConfigResource, Collection<String>> resourceToConfigNames) {
            super(ApiKeys.DESCRIBE_CONFIGS);
            this.resourceToConfigNames = Objects.requireNonNull(resourceToConfigNames, "resourceToConfigNames");
        }

        public Builder includeSynonyms(boolean includeSynonyms) {
            this.includeSynonyms = includeSynonyms;
            return this;
        }

        public Builder(Collection<ConfigResource> resources) {
            this(toResourceToConfigNames(resources));
        }

        private static Map<ConfigResource, Collection<String>> toResourceToConfigNames(Collection<ConfigResource> resources) {
            Map<ConfigResource, Collection<String>> result = new HashMap<>(resources.size());
            for (ConfigResource resource : resources)
                result.put(resource, null);
            return result;
        }

        @Override
        public DescribeConfigsRequest build(short version) {
            return new DescribeConfigsRequest(version, resourceToConfigNames, includeSynonyms);
        }
    }

    private final Map<ConfigResource, Collection<String>> resourceToConfigNames;
    private final boolean includeSynonyms;

    public DescribeConfigsRequest(short version, Map<ConfigResource, Collection<String>> resourceToConfigNames, boolean includeSynonyms) {
        super(ApiKeys.DESCRIBE_CONFIGS, version);
        this.resourceToConfigNames = Objects.requireNonNull(resourceToConfigNames, "resourceToConfigNames");
        this.includeSynonyms = includeSynonyms;
    }

    public DescribeConfigsRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_CONFIGS, version);
        Object[] resourcesArray = struct.getArray(RESOURCES_KEY_NAME);
        resourceToConfigNames = new HashMap<>(resourcesArray.length);
        for (Object resourceObj : resourcesArray) {
            Struct resourceStruct = (Struct) resourceObj;
            ConfigResource.Type resourceType = ConfigResource.Type.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);

            Object[] configNamesArray = resourceStruct.getArray(CONFIG_NAMES_KEY_NAME);
            List<String> configNames = null;
            if (configNamesArray != null) {
                configNames = new ArrayList<>(configNamesArray.length);
                for (Object configNameObj : configNamesArray)
                    configNames.add((String) configNameObj);
            }

            resourceToConfigNames.put(new ConfigResource(resourceType, resourceName), configNames);
        }
        this.includeSynonyms = struct.hasField(INCLUDE_SYNONYMS) ? struct.getBoolean(INCLUDE_SYNONYMS) : false;
    }

    public Collection<ConfigResource> resources() {
        return resourceToConfigNames.keySet();
    }

    /**
     * Return null if all config names should be returned.
     */
    public Collection<String> configNames(ConfigResource resource) {
        return resourceToConfigNames.get(resource);
    }

    public boolean includeSynonyms() {
        return includeSynonyms;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_CONFIGS.requestSchema(version()));
        List<Struct> resourceStructs = new ArrayList<>(resources().size());
        for (Map.Entry<ConfigResource, Collection<String>> entry : resourceToConfigNames.entrySet()) {
            ConfigResource resource = entry.getKey();
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.type().id());
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.name());

            String[] configNames = entry.getValue() == null ? null : entry.getValue().toArray(new String[0]);
            resourceStruct.set(CONFIG_NAMES_KEY_NAME, configNames);

            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray(new Struct[0]));
        struct.setIfExists(INCLUDE_SYNONYMS, includeSynonyms);
        return struct;
    }

    @Override
    public DescribeConfigsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
            case 1:
            case 2:
                ApiError error = ApiError.fromThrowable(e);
                Map<ConfigResource, DescribeConfigsResponse.Config> errors = new HashMap<>(resources().size());
                DescribeConfigsResponse.Config config = new DescribeConfigsResponse.Config(error,
                        Collections.emptyList());
                for (ConfigResource resource : resources())
                    errors.put(resource, config);
                return new DescribeConfigsResponse(throttleTimeMs, errors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_CONFIGS.latestVersion()));
        }
    }

    public static DescribeConfigsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeConfigsRequest(ApiKeys.DESCRIBE_CONFIGS.parseRequest(version, buffer), version);
    }
}

