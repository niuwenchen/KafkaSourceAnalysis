package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Struct;

public abstract class AbstractControlRequest extends AbstractRequest {
    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    protected static final Field.Int32 CONTROLLER_ID = new Field.Int32("controller_id", "The controller id");
    protected static final Field.Int32 CONTROLLER_EPOCH = new Field.Int32("controller_epoch", "The controller epoch");
    protected static final Field.Int64 BROKER_EPOCH = new Field.Int64("broker_epoch", "The broker epoch");

    protected final int controllerId;
    protected final int controllerEpoch;
    protected final long brokerEpoch;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        protected final int controllerId;
        protected final int controllerEpoch;
        protected final long brokerEpoch;

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
        }

    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    protected AbstractControlRequest(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
        super(api, version);
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.brokerEpoch = brokerEpoch;
    }

    protected AbstractControlRequest(ApiKeys api, Struct struct, short version) {
        super(api, version);
        this.controllerId = struct.get(CONTROLLER_ID);
        this.controllerEpoch = struct.get(CONTROLLER_EPOCH);
        this.brokerEpoch = struct.getOrElse(BROKER_EPOCH, UNKNOWN_BROKER_EPOCH);
    }

    // Used for test
    long size() {
        return toStruct().sizeOf();
    }

}
