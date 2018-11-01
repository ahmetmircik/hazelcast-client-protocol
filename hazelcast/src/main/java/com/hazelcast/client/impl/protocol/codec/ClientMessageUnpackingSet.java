/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.IBiFunction;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Unpacks client message and places it into a backing list. List is fine
 * to back this set since we know that elements extracted from clint
 * message will be unique.
 *
 * @param <E> type of elements in this set
 */
@SuppressWarnings("checkstyle:anoninnerlength")
final class ClientMessageUnpackingSet<E> extends AbstractSet<E> {

    private final int numOfElementsInResponse;

    private final ClientMessage response;
    private final SerializationService ss;
    private final IBiFunction<ClientMessage, SerializationService, E> function;

    List<E> backingList;

    private ClientMessageUnpackingSet(int numOfElementsInMessage,
                                      ClientMessage clientMessage,
                                      SerializationService ss,
                                      IBiFunction<ClientMessage, SerializationService, E> function) {
        this.numOfElementsInResponse = numOfElementsInMessage;
        this.response = clientMessage;
        this.function = function;
        this.ss = ss;
    }

    static <T> Set<T> fromClientMessageToSet(ClientMessage message,
                                                    SerializationService ss,
                                                    IBiFunction<ClientMessage, SerializationService, T> function) {
        assert message != null;
        assert function != null;

        int numOfElementsInResponse = message.getInt();

        if (numOfElementsInResponse == 0) {
            return Collections.emptySet();
        }

        return new ClientMessageUnpackingSet<T>(numOfElementsInResponse, message, ss, function);
    }

    @Override
    public int size() {
        return numOfElementsInResponse;
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            int visited;

            @Override
            public boolean hasNext() {
                return visited != numOfElementsInResponse;
            }

            @Override
            public E next() {
                if (visited >= numOfElementsInResponse) {
                    throw new NoSuchElementException();
                }

                if (backingList != null && visited < backingList.size()) {
                    return backingList.get(visited++);
                }

                if (backingList == null) {
                    backingList = new ArrayList<E>(numOfElementsInResponse);
                }

                E element = function.apply(response, ss);
                backingList.add(element);

                visited++;
                return element;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
