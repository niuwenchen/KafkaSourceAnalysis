package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.memory.MemoryPool;
import com.jackniu.kafka.common.security.auth.KafkaPrincipal;
import com.jackniu.kafka.common.security.auth.KafkaPrincipalBuilder;
import com.jackniu.kafka.common.security.auth.PlaintextAuthenticationContext;
import com.jackniu.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.function.Supplier;

public class PlaintextChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(PlaintextChannelBuilder.class);
    private final ListenerName listenerName;
    private Map<String, ?> configs;

    /**
     * Constructs a plaintext channel builder. ListenerName is non-null whenever
     * it's instantiated in the broker and null otherwise.
     */
    public PlaintextChannelBuilder(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        this.configs = configs;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
            Supplier<Authenticator> authenticatorCreator = () -> new PlaintextAuthenticator(configs, transportLayer, listenerName);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.warn("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {}

    private static class PlaintextAuthenticator implements Authenticator {
        private final PlaintextTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;

        private PlaintextAuthenticator(Map<String, ?> configs, PlaintextTransportLayer transportLayer, ListenerName listenerName) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, transportLayer, this, null, null);
            this.listenerName = listenerName;
        }

        @Override
        public void authenticate() {}

        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            // listenerName should only be null in Client mode where principal() should not be called
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            return principalBuilder.build(new PlaintextAuthenticationContext(clientAddress, listenerName.value()));
        }

        @Override
        public boolean complete() {
            return true;
        }

        @Override
        public void close() {
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }
    }

}
