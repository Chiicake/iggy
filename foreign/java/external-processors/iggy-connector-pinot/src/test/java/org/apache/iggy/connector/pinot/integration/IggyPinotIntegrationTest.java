/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.connector.pinot.integration;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class IggyPinotIntegrationTest {

    private static final int IGGY_HTTP_PORT = 3000;
    private static final int PINOT_CONTROLLER_PORT = 9000;
    private static final int PINOT_BROKER_PORT = 8099;
    private static final int PINOT_SERVER_ADMIN_PORT = 8097;

    @Container
    static final DockerComposeContainer<?> environment =
            new DockerComposeContainer<>(new File("docker-compose.test.yml"))
                    .withExposedService(
                            "iggy",
                            IGGY_HTTP_PORT,
                            Wait.forHttp("/")
                                    .forPort(IGGY_HTTP_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-controller",
                            PINOT_CONTROLLER_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_CONTROLLER_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-broker",
                            PINOT_BROKER_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_BROKER_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-server",
                            PINOT_SERVER_ADMIN_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_SERVER_ADMIN_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)));

    @Test
    void servicesAreRunning() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        String iggyUrl = baseUrl("iggy", IGGY_HTTP_PORT);
        String controllerUrl = baseUrl("pinot-controller", PINOT_CONTROLLER_PORT);
        String brokerUrl = baseUrl("pinot-broker", PINOT_BROKER_PORT);
        String serverUrl = baseUrl("pinot-server", PINOT_SERVER_ADMIN_PORT);

        assertThat(httpStatus(client, iggyUrl + "/")).isEqualTo(200);
        assertThat(httpStatus(client, controllerUrl + "/health")).isEqualTo(200);
        assertThat(httpStatus(client, brokerUrl + "/health")).isEqualTo(200);
        assertThat(httpStatus(client, serverUrl + "/health")).isEqualTo(200);
    }

    @Test
    void sendMessagesToIggy() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        String iggyUrl = baseUrl("iggy", IGGY_HTTP_PORT);
        HttpResponse<String> loginResponse = postJson(
                client,
                iggyUrl + "/users/login",
                "{\"username\":\"iggy\",\"password\":\"iggy\"}",
                null);
        assertThat(loginResponse.statusCode()).isBetween(200, 299);

        String token = loginResponse.body()
                .split("\"token\":\"")[1]
                .split("\"")[0];
        assertThat(token).isNotBlank();

        HttpResponse<String> streamResponse = postJson(
                client,
                iggyUrl + "/streams",
                "{\"stream_id\":1,\"name\":\"test-stream\"}",
                token);
        assertThat(streamResponse.statusCode()).isBetween(200, 299);

        HttpResponse<String> topicResponse = postJson(
                client,
                iggyUrl + "/streams/test-stream/topics",
                "{\"topic_id\":1,\"name\":\"test-events\",\"partitions_count\":2,"
                        + "\"compression_algorithm\":\"none\",\"message_expiry\":0,\"max_topic_size\":0}",
                token);
        assertThat(topicResponse.statusCode()).isBetween(200, 299);

        HttpResponse<String> groupResponse = postJson(
                client,
                iggyUrl + "/streams/test-stream/topics/test-events/consumer-groups",
                "{\"name\":\"pinot-integration-test\"}",
                token);
        assertThat(groupResponse.statusCode()).isBetween(200, 299);

        String partitionValue = Base64.getEncoder().encodeToString(new byte[] {0, 0, 0, 0});
        for (int i = 1; i <= 10; i++) {
            long timestamp = System.currentTimeMillis();
            String payloadJson = "{"
                    + "\"userId\":\"user" + i + "\","
                    + "\"eventType\":\"test_event\","
                    + "\"deviceType\":\"desktop\","
                    + "\"duration\":" + (i * 100) + ","
                    + "\"timestamp\":" + timestamp
                    + "}";
            String payload = Base64.getEncoder()
                    .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
            String body = "{\"partitioning\":{\"kind\":\"partition_id\",\"value\":\"" + partitionValue + "\"},"
                    + "\"messages\":[{\"payload\":\"" + payload + "\"}]}";
            HttpResponse<String> sendResponse = postJson(
                    client,
                    iggyUrl + "/streams/test-stream/topics/test-events/messages",
                    body,
                    token);
            assertThat(sendResponse.statusCode()).isBetween(200, 299);
        }
    }

    private static String baseUrl(String service, int port) {
        String host = environment.getServiceHost(service, port);
        Integer mappedPort = environment.getServicePort(service, port);
        return "http://" + host + ":" + mappedPort;
    }

    private static int httpStatus(HttpClient client, String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode();
        } catch (Exception e) {
            throw new RuntimeException("HTTP request failed for " + url, e);
        }
    }

    private static HttpResponse<String> postJson(
            HttpClient client,
            String url,
            String json,
            String token) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

}
