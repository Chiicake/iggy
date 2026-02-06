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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IggyPinotIntegrationTest {

    private static final int IGGY_HTTP_PORT = 3000;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int PINOT_CONTROLLER_PORT = 9000;
    private static final int PINOT_BROKER_PORT = 8099;
    private static final int PINOT_SERVER_ADMIN_PORT = 8097;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration INGESTION_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Pattern COUNT_PATTERN = Pattern.compile("\"rows\"\\s*:\\s*\\[\\s*\\[\\s*(\\d+)");

    private static HttpClient client;
    private static String iggyUrl;
    private static String controllerUrl;
    private static String brokerUrl;
    private static String serverUrl;
    private static String token;

    @Container
    static final DockerComposeContainer<?> environment =
            new DockerComposeContainer<>(new File("docker-compose.test.yml"))
                    .withServices("iggy", "zookeeper", "pinot-controller", "pinot-broker", "pinot-server")
                    .withExposedService(
                            "iggy",
                            IGGY_HTTP_PORT,
                            Wait.forHttp("/")
                                    .forPort(IGGY_HTTP_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "zookeeper",
                            ZOOKEEPER_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
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

    @BeforeAll
    static void init() {
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        iggyUrl = baseUrl("iggy", IGGY_HTTP_PORT);
        controllerUrl = baseUrl("pinot-controller", PINOT_CONTROLLER_PORT);
        brokerUrl = baseUrl("pinot-broker", PINOT_BROKER_PORT);
        serverUrl = baseUrl("pinot-server", PINOT_SERVER_ADMIN_PORT);
    }

    @Test
    @Order(1)
    void step1_servicesAreRunning() {
        System.out.println("Step 1: Checking services");
        System.out.println("Iggy: " + iggyUrl);
        System.out.println("Pinot Controller: " + controllerUrl);
        System.out.println("Pinot Broker: " + brokerUrl);
        System.out.println("Pinot Server: " + serverUrl);
        assertThat(httpStatus(client, iggyUrl + "/")).isEqualTo(200);
        assertThat(httpStatus(client, controllerUrl + "/health")).isEqualTo(200);
        assertThat(httpStatus(client, brokerUrl + "/health")).isEqualTo(200);
        assertThat(httpStatus(client, serverUrl + "/health")).isEqualTo(200);
    }

    @Test
    @Order(2)
    void step2_loginToIggy() throws Exception {
        System.out.println("Step 2: Login to Iggy");
        HttpResponse<String> loginResponse = postJson(
                client,
                iggyUrl + "/users/login",
                "{\"username\":\"iggy\",\"password\":\"iggy\"}",
                null);
        System.out.println("Login response: " + loginResponse.statusCode());
        System.out.println(loginResponse.body());
        assertThat(loginResponse.statusCode()).isBetween(200, 299);
        token = extractToken(loginResponse.body());
        assertThat(token).isNotBlank();
    }

    @Test
    @Order(3)
    void step3_createStream() throws Exception {
        System.out.println("Step 3: Create stream");
        HttpResponse<String> streamResponse = postJson(
                client,
                iggyUrl + "/streams",
                "{\"stream_id\":1,\"name\":\"test-stream\"}",
                token);
        System.out.println("Create stream response: " + streamResponse.statusCode());
        System.out.println(streamResponse.body());
        assertThat(streamResponse.statusCode()).isBetween(200, 299);
    }

    @Test
    @Order(4)
    void step4_createTopic() throws Exception {
        System.out.println("Step 4: Create topic");
        HttpResponse<String> topicResponse = postJson(
                client,
                iggyUrl + "/streams/test-stream/topics",
                "{\"topic_id\":1,\"name\":\"test-events\",\"partitions_count\":2,"
                        + "\"compression_algorithm\":\"none\",\"message_expiry\":0,\"max_topic_size\":0}",
                token);
        System.out.println("Create topic response: " + topicResponse.statusCode());
        System.out.println(topicResponse.body());
        assertThat(topicResponse.statusCode()).isBetween(200, 299);
    }

    @Test
    @Order(5)
    void step5_createConsumerGroup() throws Exception {
        System.out.println("Step 5: Create consumer group");
        HttpResponse<String> groupResponse = postJson(
                client,
                iggyUrl + "/streams/test-stream/topics/test-events/consumer-groups",
                "{\"name\":\"pinot-integration-test\"}",
                token);
        System.out.println("Create consumer group response: " + groupResponse.statusCode());
        System.out.println(groupResponse.body());
        assertThat(groupResponse.statusCode()).isBetween(200, 299);
    }

    @Test
    @Order(6)
    void step6_createSchema() throws Exception {
        System.out.println("Step 6: Create schema");
        String schemaJson = Files.readString(resolvePath("deployment/schema.json"), StandardCharsets.UTF_8);
        HttpResponse<String> schemaResponse = postJson(
                client,
                controllerUrl + "/schemas",
                schemaJson,
                null);
        System.out.println("Create schema response: " + schemaResponse.statusCode());
        System.out.println(schemaResponse.body());
        assertThat(schemaResponse.statusCode()).isBetween(200, 299);
    }

    @Test
    @Order(7)
    void step7_createTable() throws Exception {
        System.out.println("Step 7: Create table");
        String tableJson = Files.readString(resolvePath("deployment/table.json"), StandardCharsets.UTF_8);
        HttpResponse<String> tableResponse = postJson(
                client,
                controllerUrl + "/tables",
                tableJson,
                null);
        System.out.println("Create table response: " + tableResponse.statusCode());
        System.out.println(tableResponse.body());
        assertThat(tableResponse.statusCode()).isBetween(200, 299);
    }

    @Test
    @Order(8)
    void step8_sendMessages() throws Exception {
        System.out.println("Step 8: Send messages");
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
            System.out.println("Send message " + i + " response: " + sendResponse.statusCode());
            System.out.println(sendResponse.body());
            assertThat(sendResponse.statusCode()).isBetween(200, 299);
        }
    }

    @Test
    @Order(9)
    void step9_pollPinotForIngestion() throws Exception {
        System.out.println("Step 9: Poll Pinot for ingestion");
        String queryBody = "{\"sql\":\"SELECT COUNT(*) FROM test_events_REALTIME\"}";
        long deadline = System.currentTimeMillis() + INGESTION_TIMEOUT.toMillis();
        long count = -1;

        while (System.currentTimeMillis() < deadline) {
            HttpResponse<String> response = postJson(client, brokerUrl + "/query/sql", queryBody, null);
            System.out.println("Query response: " + response.statusCode());
            System.out.println(response.body());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                count = extractCount(response.body());
                if (count > 0) {
                    break;
                }
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        assertThat(count)
                .withFailMessage("No rows ingested within timeout")
                .isGreaterThan(0);
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

    private static Path resolvePath(String relativePath) throws IOException {
        Path direct = Path.of(relativePath);
        if (Files.exists(direct)) {
            return direct;
        }
        Path fallback = Path.of("external-processors/iggy-connector-pinot").resolve(relativePath);
        if (Files.exists(fallback)) {
            return fallback;
        }
        throw new IOException("File not found: " + relativePath);
    }

    private static HttpResponse<String> postJson(
            HttpClient client,
            String url,
            String json,
            String token) throws Exception {
        int maxAttempts = 3;
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(REQUEST_TIMEOUT)
                        .header("Accept", "*/*")
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
                if (token != null) {
                    builder.header("Authorization", "Bearer " + token);
                }
                HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
                if (!shouldRetry(response.statusCode()) || attempt == maxAttempts) {
                    return response;
                }
                System.out.println("Request failed with status " + response.statusCode() + ", retrying...");
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (attempt == maxAttempts) {
                    throw e;
                }
                System.out.println("Request error: " + e.getMessage() + ", retrying...");
            }
            Thread.sleep(2000);
        }
        throw new IllegalStateException("Request retries exhausted");
    }

    private static boolean shouldRetry(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    private static String extractToken(String body) {
        int idx = body.indexOf("\"token\":\"");
        if (idx < 0) {
            throw new IllegalStateException("Token not found in login response");
        }
        int start = idx + "\"token\":\"".length();
        int end = body.indexOf("\"", start);
        if (end < 0) {
            throw new IllegalStateException("Token not terminated in login response");
        }
        return body.substring(start, end);
    }

    private static long extractCount(String body) {
        Matcher matcher = COUNT_PATTERN.matcher(body);
        if (!matcher.find()) {
            return -1;
        }
        try {
            return Long.parseLong(matcher.group(1));
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}
