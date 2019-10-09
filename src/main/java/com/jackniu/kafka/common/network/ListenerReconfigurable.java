package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.Reconfigurable;

public interface ListenerReconfigurable  extends Reconfigurable {

    /**
     * Returns the listener name associated with this reconfigurable. Listener-specific
     * configs corresponding to this listener name are provided for reconfiguration.
     */
    ListenerName listenerName();
}

