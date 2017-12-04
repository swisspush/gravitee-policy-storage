/**
 * Copyright (C) 2018 The Gravitee team (http://gravitee.io)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.policy.storage;

import io.gravitee.common.http.HttpHeaders;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.Invoker;
import io.gravitee.gateway.api.Request;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.proxy.ProxyConnection;
import io.gravitee.gateway.api.proxy.ProxyResponse;
import io.gravitee.gateway.api.stream.ReadStream;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Invoker requesting the rest storage via the event bus.
 *
 * @author Laurent Bovet <laurent.bovet@swisspush.org>
 */
public class StorageInvoker implements Invoker {

    private String storageAddress;

    public StorageInvoker(String storageAddress) {
        this.storageAddress = storageAddress;
    }

    @Override
    public Request invoke(ExecutionContext executionContext, Request serverRequest, ReadStream<Buffer> stream, Handler<ProxyConnection> connectionHandler) {
        // Prepare head part
        String head = new JsonObject()
                .put("method", serverRequest.method().toString())
                .put("uri", serverRequest.uri())
                .put("headers", new JsonArray(
                        serverRequest.headers()
                        .entrySet().stream()
                        .flatMap( entry -> entry.getValue().stream()
                                .map( value -> Arrays.asList(entry.getKey(), value)))
                        .collect(Collectors.toList()))).encode();

        ProxyConnection proxyConnection = new ProxyConnection() {
            private Handler<ProxyResponse> responseHandler;
            private Buffer requestData = Buffer.buffer(head);

            @Override
            public WriteStream<Buffer> write(Buffer content) {
                requestData.appendBuffer(content);
                return this;
            }

            @Override
            public void end() {
                // Perform the request once all request data has been received
                executionContext.getComponent(Vertx.class).eventBus()
                    .send(storageAddress, vertxBuffer(requestData), (AsyncResult<Message<io.vertx.core.buffer.Buffer>> message) -> {
                        io.vertx.core.buffer.Buffer responseData = message.result().body();
                        int headerLength = responseData.getInt(0);
                        final JsonObject head = new JsonObject(responseData.getString(4, headerLength+4));
                        HttpHeaders headers = new HttpHeaders();
                        head.getJsonArray("headers").forEach( (headerObject) -> {
                            String key = ((JsonArray)headerObject).getString(0);
                            String value = ((JsonArray)headerObject).getString(0);
                            headers.add(key, value);
                        });

                        responseHandler.handle(new ProxyResponse() {
                            private Handler<Buffer> bodyHandler;
                            private Handler<Void> endHandler;

                            @Override
                            public int status() {
                                return head.getInteger("statusCode");
                            }

                            @Override
                            public HttpHeaders headers() {
                                return headers;
                            }

                            @Override
                            public ReadStream<Buffer> bodyHandler(Handler<Buffer> bodyHandler) {
                                this.bodyHandler = bodyHandler;
                                return this;
                            }

                            @Override
                            public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
                                this.endHandler = endHandler;
                                return this;
                            }

                            @Override
                            public ReadStream<Buffer> resume() {
                                Buffer responseBody = Buffer.buffer(responseData.getBuffer(4+headerLength, responseData.length()).getBytes());
                                if(responseBody.length() > 0) {
                                    bodyHandler.handle(responseBody);
                                }
                                endHandler.handle(null);
                                return this;
                            }
                        });
                    });
            }

            @Override
            public ProxyConnection cancel() {
                return this;
            }

            @Override
            public ProxyConnection exceptionHandler(Handler<Throwable> exceptionHandler) {
                return this;
            }

            @Override
            public ProxyConnection responseHandler(Handler<ProxyResponse> responseHandler) {
                this.responseHandler = responseHandler;
                return this;
            }
        };

        // Plug underlying stream to connection stream
        stream
                .bodyHandler(proxyConnection::write)
                .endHandler(aVoid -> proxyConnection.end());

        // Resume the incoming request to handle content and end
        serverRequest.resume();
        return serverRequest;
    }

    private io.vertx.core.buffer.Buffer requestBuffer(String string) {
        io.vertx.core.buffer.Buffer head = io.vertx.core.buffer.Buffer.buffer(string);
        io.vertx.core.buffer.Buffer request = io.vertx.core.buffer.Buffer.buffer(4+head.length());
        request.setInt(0, head.length()).appendBuffer(head);
        return request;
    }

    private io.vertx.core.buffer.Buffer vertxBuffer(Buffer buffer) {
        Object nativeBuffer = buffer.getNativeBuffer();
        if(buffer.getNativeBuffer() instanceof io.vertx.core.buffer.Buffer) {
            return (io.vertx.core.buffer.Buffer)nativeBuffer;
        } else {
            return io.vertx.core.buffer.Buffer.buffer(buffer.getBytes());
        }
    }
}
