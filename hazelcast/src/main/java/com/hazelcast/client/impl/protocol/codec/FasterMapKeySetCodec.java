package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.core.IBiFunction;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;

/**
 * @since 1.0
 * update 1.0
 */
public final class FasterMapKeySetCodec {

    public static final MapMessageType REQUEST_TYPE = MapMessageType.MAP_KEYSET;
    public static final int RESPONSE_TYPE = 106;

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
        clientMessage.setOperationName("Map.keySet");
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
        public java.util.List<com.hazelcast.nio.serialization.Data> response;

        /**
         * @since 1.0
         */
        public static int calculateDataSize(java.util.Collection<com.hazelcast.nio.serialization.Data> response) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (com.hazelcast.nio.serialization.Data response_item : response) {
                dataSize += ParameterUtil.calculateDataSize(response_item);
            }
            return dataSize;
        }
    }

    /**
     * @since 1.0
     */
    public static ClientMessage encodeResponse(java.util.Collection<com.hazelcast.nio.serialization.Data> response) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(response);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
        clientMessage.set(response.size());
        for (com.hazelcast.nio.serialization.Data response_item : response) {
            clientMessage.set(response_item);
        }
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    static final IBiFunction<ClientMessage, SerializationService, Object> TO_KEY_FUNCTION
            = new IBiFunction<ClientMessage, SerializationService, Object>() {
        @Override
        public Object apply(ClientMessage response, SerializationService ss) {
            return ss.toObject(response.getData());
        }
    };

    public static Collection<Object> decodeResponse(ClientMessage clientMessage, SerializationService ss) {
        return ClientMessageUnpackingSet.fromClientMessageToSet(clientMessage, ss, TO_KEY_FUNCTION);
    }


}
