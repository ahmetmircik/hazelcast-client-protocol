package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.core.IBiFunction;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;
import java.util.Set;

/**
 * @since 1.0
 * update 1.0
 */
public final class FasterMapEntrySetCodec {

    public static final MapMessageType REQUEST_TYPE = MapMessageType.MAP_ENTRYSET;
    public static final int RESPONSE_TYPE = 117;

    //************************ REQUEST *************************//

    public static class RequestParameters {
        public static final MapMessageType TYPE = REQUEST_TYPE;

        /**
         * @since 1.0
         */
        public java.lang.String name;

        /**
         * @since 1.0
         */
        public static int calculateDataSize(java.lang.String name) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += ParameterUtil.calculateDataSize(name);
            return dataSize;
        }
    }

    /**
     * @since 1.0
     */
    public static ClientMessage encodeRequest(java.lang.String name) {
        final int requiredDataSize = RequestParameters.calculateDataSize(name);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.entrySet");
        clientMessage.set(name);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
        java.lang.String name = null;
        name = clientMessage.getStringUtf8();
        parameters.name = name;

        return parameters;
    }

    //************************ RESPONSE *************************//

    public static class ResponseParameters {
        /**
         * @since 1.0
         */
        public java.util.List<java.util.Map.Entry<com.hazelcast.nio.serialization.Data, com.hazelcast.nio.serialization.Data>> response;

        /**
         * @since 1.0
         */
        public static int calculateDataSize(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.serialization.Data, com.hazelcast.nio.serialization.Data>> response) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (java.util.Map.Entry<com.hazelcast.nio.serialization.Data, com.hazelcast.nio.serialization.Data> response_item : response) {
                com.hazelcast.nio.serialization.Data response_itemKey = response_item.getKey();
                com.hazelcast.nio.serialization.Data response_itemVal = response_item.getValue();
                dataSize += ParameterUtil.calculateDataSize(response_itemKey);
                dataSize += ParameterUtil.calculateDataSize(response_itemVal);
            }
            return dataSize;
        }
    }

    /**
     * @since 1.0
     */
    public static ClientMessage encodeResponse(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.serialization.Data, com.hazelcast.nio.serialization.Data>> response) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(response);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
        clientMessage.set(response.size());
        for (java.util.Map.Entry<com.hazelcast.nio.serialization.Data, com.hazelcast.nio.serialization.Data> response_item : response) {
            com.hazelcast.nio.serialization.Data response_itemKey = response_item.getKey();
            com.hazelcast.nio.serialization.Data response_itemVal = response_item.getValue();
            clientMessage.set(response_itemKey);
            clientMessage.set(response_itemVal);
        }
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    static final IBiFunction<ClientMessage, SerializationService, Map.Entry> TO_ENTRY_FUNCTION
            = new IBiFunction<ClientMessage, SerializationService, Map.Entry>() {
        @Override
        public Map.Entry apply(ClientMessage response, SerializationService ss) {
            Data dataKey = response.getData();
            Data dataValue = response.getData();
            return new LazyMapEntry(dataKey, dataValue, ((InternalSerializationService) ss));
        }
    };

    public static Set decodeResponse(ClientMessage clientMessage, SerializationService ss) {
        return ClientMessageUnpackingSet.fromClientMessageToSet(clientMessage, ss, TO_ENTRY_FUNCTION);
    }
}
