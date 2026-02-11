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

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IggyPinotIntegrationTest {

    private static final int IGGY_HTTP_PORT = 3000;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int PINOT_CONTROLLER_PORT = 9000;
    private static final int PINOT_BROKER_PORT = 8099;
    private static final int PINOT_SERVER_ADMIN_PORT = 8097;

    private static final String SERVICE_IGGY = "iggy";
    private static final String SERVICE_ZOOKEEPER = "zookeeper";
    private static final String SERVICE_PINOT_CONTROLLER = "pinot-controller";
    private static final String SERVICE_PINOT_BROKER = "pinot-broker";
    private static final String SERVICE_PINOT_SERVER = "pinot-server";

    private static final String STREAM_NAME = "test-stream";
    private static final String TOPIC_NAME = "test-events";
    private static final String CONSUMER_GROUP_NAME = "pinot-integration-test";
    private static final String TABLE_NAME = "test_events_REALTIME";
    private static final String COUNT_SQL = "SELECT COUNT(*) FROM " + TABLE_NAME;
    private static final String PARTITION_VALUE_BASE64 = Base64.getEncoder().encodeToString(new byte[] {0, 0, 0, 0});

    private static final Duration INGESTION_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration BULK_INGESTION_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Duration REQUEST_RETRY_DELAY = Duration.ofSeconds(2);

    private static final int MESSAGES_TO_SEND = 10;
    private static final int BULK_MESSAGES_TO_SEND = 120;
    private static final int BULK_BATCH_SIZE = 20;
    private static final int LOG_TAIL_LINES = 80;
    private static final int REQUEST_MAX_ATTEMPTS = 3;

    private String iggyUrl;
    private String controllerUrl;
    private String brokerUrl;
    private String serverUrl;
    private String token;

    @Container
    static final DockerComposeContainer<?> environment =
            new DockerComposeContainer<>(new File("docker-compose.yml"))
                    .withServices(
                            SERVICE_IGGY,
                            SERVICE_ZOOKEEPER,
                            SERVICE_PINOT_CONTROLLER,
                            SERVICE_PINOT_BROKER,
                            SERVICE_PINOT_SERVER)
                    .withExposedService(
                            SERVICE_IGGY,
                            IGGY_HTTP_PORT,
                            Wait.forHttp("/")
                                    .forPort(IGGY_HTTP_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            SERVICE_ZOOKEEPER,
                            ZOOKEEPER_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            SERVICE_PINOT_CONTROLLER,
                            PINOT_CONTROLLER_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_CONTROLLER_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            SERVICE_PINOT_BROKER,
                            PINOT_BROKER_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_BROKER_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            SERVICE_PINOT_SERVER,
                            PINOT_SERVER_ADMIN_PORT,
                            Wait.forHttp("/health")
                                    .forPort(PINOT_SERVER_ADMIN_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(3)));

    @BeforeAll
    void init() {
        iggyUrl = "http://" + environment.getServiceHost(SERVICE_IGGY, IGGY_HTTP_PORT)
                + ":" + environment.getServicePort(SERVICE_IGGY, IGGY_HTTP_PORT);
        controllerUrl = "http://" + environment.getServiceHost(SERVICE_PINOT_CONTROLLER, PINOT_CONTROLLER_PORT)
                + ":" + environment.getServicePort(SERVICE_PINOT_CONTROLLER, PINOT_CONTROLLER_PORT);
        brokerUrl = "http://" + environment.getServiceHost(SERVICE_PINOT_BROKER, PINOT_BROKER_PORT)
                + ":" + environment.getServicePort(SERVICE_PINOT_BROKER, PINOT_BROKER_PORT);
        serverUrl = "http://" + environment.getServiceHost(SERVICE_PINOT_SERVER, PINOT_SERVER_ADMIN_PORT)
                + ":" + environment.getServicePort(SERVICE_PINOT_SERVER, PINOT_SERVER_ADMIN_PORT);
    }

    @Test
    @Order(1)
    void testServicesAreRunning() {
        assertEquals(200, RestAssured.given().accept("*/*").when().get(iggyUrl + "/").andReturn().statusCode());
        assertEquals(200, RestAssured.given().accept("*/*").when().get(controllerUrl + "/health").andReturn().statusCode());
        assertEquals(200, RestAssured.given().accept("*/*").when().get(brokerUrl + "/health").andReturn().statusCode());
        assertEquals(200, RestAssured.given().accept("*/*").when().get(serverUrl + "/health").andReturn().statusCode());
    }

    @Test
    @Order(2)
    void testLoginToIggy() {
        Response response = postJson(
                iggyUrl + "/users/login",
                "{\"username\":\"iggy\",\"password\":\"iggy\"}",
                null);
        assertSuccess("Login to Iggy", response);
        token = extractToken(response);
    }

    @Test
    @Order(3)
    void testCreateStream() {
        Response response = postJson(
                iggyUrl + "/streams",
                "{\"stream_id\":1,\"name\":\"" + STREAM_NAME + "\"}",
                token);
        assertSuccess("Create stream", response);
    }

    @Test
    @Order(4)
    void testCreateTopic() {
        Response response = postJson(
                iggyUrl + "/streams/" + STREAM_NAME + "/topics",
                "{\"topic_id\":1,\"name\":\"" + TOPIC_NAME + "\",\"partitions_count\":2,"
                        + "\"compression_algorithm\":\"none\",\"message_expiry\":0,\"max_topic_size\":0}",
                token);
        assertSuccess("Create topic", response);
    }

    @Test
    @Order(5)
    void testCreateConsumerGroup() {
        Response response = postJson(
                iggyUrl + "/streams/" + STREAM_NAME + "/topics/" + TOPIC_NAME + "/consumer-groups",
                "{\"name\":\"" + CONSUMER_GROUP_NAME + "\"}",
                token);
        assertSuccess("Create consumer group", response);
    }

    @Test
    @Order(6)
    void testEnsurePinotTenants() throws Exception {
        assertTenantAdded("BROKER");
        assertTenantAdded("SERVER");
    }

    @Test
    @Order(7)
    void testCreateSchema() throws Exception {
        String schemaJson = Files.readString(resolvePath("deployment/schema.json"), StandardCharsets.UTF_8);
        Response response = postJson(controllerUrl + "/schemas", schemaJson, null);
        assertSuccess("Create schema", response);
    }

    @Test
    @Order(8)
    void testCreateTable() throws Exception {
        String tableJson = Files.readString(resolvePath("deployment/table.json"), StandardCharsets.UTF_8);
        Response response = postJson(controllerUrl + "/tables", tableJson, null);
        assertSuccess("Create table", response);
    }

    @Test
    @Order(9)
    void testSendMessages() {
        for (int i = 1; i <= MESSAGES_TO_SEND; i++) {
            String payloadJson =
                    eventPayloadJson("user" + i, "test_event", "desktop", i * 100L, System.currentTimeMillis());
            Response response = sendEventPayloads(List.of(payloadJson));
            assertSuccess("Send message " + i, response);
        }
    }

    @Test
    @Order(10)
    void testPollPinotForIngestion() {
        long count = waitForCountAtLeast(MESSAGES_TO_SEND, INGESTION_TIMEOUT);
        assertTrue(count >= MESSAGES_TO_SEND, "Expected at least " + MESSAGES_TO_SEND + " rows, but got " + count);
    }

    @Test
    @Order(11)
    void testValidateJsonParsingAndSchemaMapping() {
        String userId = "schema-user-" + System.currentTimeMillis();
        long expectedDuration = 4242L;
        long expectedTimestamp = System.currentTimeMillis();

        String payloadJson = eventPayloadJson(userId, "schema_event", "mobile", expectedDuration, expectedTimestamp);
        Response sendResponse = sendEventPayloads(List.of(payloadJson));
        assertSuccess("Send schema mapping test message", sendResponse);

        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE userId = '" + userId + "' LIMIT 1";
        Response queryResponse = waitForQueryRow(sql, INGESTION_TIMEOUT);
        List<String> columnNames = queryResponse.jsonPath().getList("resultTable.dataSchema.columnNames");
        List<Object> row = queryResponse.jsonPath().getList("resultTable.rows[0]");

        assertNotNull(columnNames, "Missing column names in query response: " + queryResponse.asString());
        assertNotNull(row, "Missing row in query response: " + queryResponse.asString());

        int userIdIndex = columnNames.indexOf("userId");
        int eventTypeIndex = columnNames.indexOf("eventType");
        int deviceTypeIndex = columnNames.indexOf("deviceType");
        int durationIndex = columnNames.indexOf("duration");
        int timestampIndex = columnNames.indexOf("timestamp");

        assertTrue(userIdIndex >= 0, "Missing userId column: " + columnNames);
        assertTrue(eventTypeIndex >= 0, "Missing eventType column: " + columnNames);
        assertTrue(deviceTypeIndex >= 0, "Missing deviceType column: " + columnNames);
        assertTrue(durationIndex >= 0, "Missing duration column: " + columnNames);
        assertTrue(timestampIndex >= 0, "Missing timestamp column: " + columnNames);

        int minSize = Math.max(
                timestampIndex,
                Math.max(durationIndex, Math.max(deviceTypeIndex, Math.max(eventTypeIndex, userIdIndex)))) + 1;
        assertTrue(row.size() >= minSize, "Expected row size >= " + minSize + ", actual=" + row.size());

        assertEquals(userId, String.valueOf(row.get(userIdIndex)));
        assertEquals("schema_event", String.valueOf(row.get(eventTypeIndex)));
        assertEquals("mobile", String.valueOf(row.get(deviceTypeIndex)));
        assertEquals(expectedDuration, toLong(row.get(durationIndex)));
        assertEquals(expectedTimestamp, toLong(row.get(timestampIndex)));
    }

    @Test
    @Order(12)
    void testBulkMessageIngestion() {
        Response baselineCountResponse = postJson(
                brokerUrl + "/query/sql",
                "{\"sql\":\"" + escapeJson(COUNT_SQL) + "\"}",
                null);
        assertSuccess("Fetch current row count", baselineCountResponse);
        long baselineCount = toLong(baselineCountResponse.jsonPath().get("resultTable.rows[0][0]"));
        String userPrefix = "bulk-user-" + System.currentTimeMillis() + "-";

        int sent = 0;
        while (sent < BULK_MESSAGES_TO_SEND) {
            int batchSize = Math.min(BULK_BATCH_SIZE, BULK_MESSAGES_TO_SEND - sent);
            List<String> payloads = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                int index = sent + i;
                payloads.add(eventPayloadJson(
                        userPrefix + index,
                        "bulk_event",
                        "mobile",
                        1000L + index,
                        System.currentTimeMillis()));
            }

            Response response = sendEventPayloads(payloads);
            int batchNumber = (sent / BULK_BATCH_SIZE) + 1;
            assertSuccess("Send bulk batch " + batchNumber, response);
            sent += batchSize;
        }

        long expectedCount = baselineCount + BULK_MESSAGES_TO_SEND;
        long finalCount = waitForCountAtLeast(expectedCount, BULK_INGESTION_TIMEOUT);
        assertTrue(finalCount >= expectedCount, "Expected at least " + expectedCount + " rows, but got " + finalCount);
    }

    // Resolves deployment file path for both module-root and repo-root test execution.
    private Path resolvePath(String relativePath) throws IOException {
        Path direct = Path.of(relativePath);
        if (Files.exists(direct)) {
            return direct;
        }
        // Support running tests from either module root or repository root.
        Path fallback = Path.of("external-processors/iggy-connector-pinot").resolve(relativePath);
        if (Files.exists(fallback)) {
            return fallback;
        }
        throw new IOException("File not found: " + relativePath);
    }

    // Sends JSON POST requests with retry to tolerate startup-time transient failures.
    private Response postJson(String url, String json, String authToken) {
        // Retry transient failures while dependent containers and endpoints stabilize.
        for (int attempt = 1; attempt <= REQUEST_MAX_ATTEMPTS; attempt++) {
            try {
                RequestSpecification request = RestAssured.given()
                        .accept("*/*")
                        .contentType("application/json")
                        .body(json);
                if (authToken != null) {
                    request.header("Authorization", "Bearer " + authToken);
                }
                Response response = request
                        .when()
                        .post(url)
                        .andReturn();
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return response;
                }
                if (attempt == REQUEST_MAX_ATTEMPTS) {
                    return response;
                }
            } catch (RuntimeException e) {
                if (attempt == REQUEST_MAX_ATTEMPTS) {
                    throw e;
                }
            }
            try {
                Thread.sleep(REQUEST_RETRY_DELAY.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread interrupted while waiting", e);
            }
        }
        throw new IllegalStateException("Request retries exhausted");
    }

    // Asserts that an HTTP operation completed with any 2xx status code.
    private void assertSuccess(String action, Response response) {
        int statusCode = response.statusCode();
        assertTrue(
                statusCode >= 200 && statusCode < 300,
                action + " failed, status=" + statusCode + ", body=" + response.asString());
    }

    // Polls Pinot count query until expected row count is visible or timeout occurs.
    private long waitForCountAtLeast(long expectedCount, Duration timeout) {
        String queryBody = "{\"sql\":\"" + escapeJson(COUNT_SQL) + "\"}";
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long count = -1;
        int lastStatus = -1;
        String lastQueryResponse = "";

        // Pinot ingestion is asynchronous, so poll until rows become query-visible.
        while (System.currentTimeMillis() < deadline) {
            Response response = postJson(brokerUrl + "/query/sql", queryBody, null);
            lastStatus = response.statusCode();
            lastQueryResponse = response.asString();
            if (lastStatus >= 200 && lastStatus < 300) {
                count = toLong(response.jsonPath().get("resultTable.rows[0][0]"));
                if (count >= expectedCount) {
                    return count;
                }
            }
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread interrupted while waiting", e);
            }
        }

        throw new AssertionError(
                "Expected at least " + expectedCount + " ingested rows, but got " + count
                        + ", lastStatus=" + lastStatus
                        + ", lastQueryResponse=" + lastQueryResponse
                        + System.lineSeparator()
                        + diagnosticLogs());
    }

    // Polls Pinot query endpoint until at least one result row is returned.
    private Response waitForQueryRow(String sql, Duration timeout) {
        String queryBody = "{\"sql\":\"" + escapeJson(sql) + "\"}";
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int lastStatus = -1;
        String lastBody = "";

        while (System.currentTimeMillis() < deadline) {
            Response response = postJson(brokerUrl + "/query/sql", queryBody, null);
            lastStatus = response.statusCode();
            lastBody = response.asString();
            if (lastStatus >= 200 && lastStatus < 300) {
                List<?> exceptions = response.jsonPath().getList("exceptions");
                if (exceptions != null && !exceptions.isEmpty()) {
                    throw new AssertionError(
                            "Query returned parsing/execution errors. sql=" + sql
                                    + ", response=" + response.asString()
                                    + System.lineSeparator()
                                    + diagnosticLogs());
                }
                List<?> rows = response.jsonPath().getList("resultTable.rows");
                if (rows != null && !rows.isEmpty()) {
                    return response;
                }
            }
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread interrupted while waiting", e);
            }
        }

        throw new AssertionError(
                "Query did not return rows in time. sql=" + sql
                        + ", lastStatus=" + lastStatus
                        + ", lastBody=" + lastBody
                        + System.lineSeparator()
                        + diagnosticLogs());
    }

    // Escapes SQL text so it can be embedded safely inside JSON payloads.
    private String escapeJson(String raw) {
        return raw.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    // Builds event payload JSON that matches schema fields used by Pinot.
    private String eventPayloadJson(
            String userId,
            String eventType,
            String deviceType,
            long duration,
            long timestamp) {
        return "{"
                + "\"userId\":\"" + userId + "\","
                + "\"eventType\":\"" + eventType + "\","
                + "\"deviceType\":\"" + deviceType + "\","
                + "\"duration\":" + duration + ","
                + "\"timestamp\":" + timestamp
                + "}";
    }

    // Sends one or more encoded event payloads to Iggy test topic.
    private Response sendEventPayloads(List<String> payloadJsonMessages) {
        StringBuilder messages = new StringBuilder();
        for (int i = 0; i < payloadJsonMessages.size(); i++) {
            if (i > 0) {
                messages.append(",");
            }
            // Iggy HTTP API expects message payloads as base64-encoded bytes.
            String payload = Base64.getEncoder()
                    .encodeToString(payloadJsonMessages.get(i).getBytes(StandardCharsets.UTF_8));
            messages.append("{\"payload\":\"").append(payload).append("\"}");
        }

        // Pin messages to partition 0 to keep ordering deterministic for the test.
        String body = "{\"partitioning\":{\"kind\":\"partition_id\",\"value\":\"" + PARTITION_VALUE_BASE64 + "\"},"
                + "\"messages\":[" + messages + "]}";
        return postJson(
                iggyUrl + "/streams/" + STREAM_NAME + "/topics/" + TOPIC_NAME + "/messages",
                body,
                token);
    }

    // Ensures Pinot DefaultTenant is present for the requested role.
    private void assertTenantAdded(String role) throws Exception {
        org.testcontainers.containers.Container.ExecResult result = findContainer(SERVICE_PINOT_CONTROLLER)
                .execInContainer(
                        "bin/pinot-admin.sh",
                        "AddTenant",
                        "-name", "DefaultTenant",
                        "-role", role,
                        "-instanceCount", "1",
                        "-controllerHost", "localhost",
                        "-controllerPort", "9000");
        assertEquals(
                0,
                result.getExitCode(),
                "AddTenant failed for role=" + role
                        + System.lineSeparator()
                        + "stdout:"
                        + System.lineSeparator()
                        + result.getStdout()
                        + System.lineSeparator()
                        + "stderr:"
                        + System.lineSeparator()
                        + result.getStderr());
    }

    private ContainerState findContainer(String serviceName) {
        Optional<ContainerState> byInstance = environment.getContainerByServiceName(serviceName + "_1");
        if (byInstance.isPresent()) {
            return byInstance.get();
        }
        return environment.getContainerByServiceName(serviceName)
                .orElseThrow(() -> new IllegalStateException("Container not found for service=" + serviceName));
    }

    // Collects tail logs from key services for richer failure diagnostics.
    private String diagnosticLogs() {
        return "=== iggy logs ===\n" + tailLogs(SERVICE_IGGY)
                + "\n=== pinot-controller logs ===\n" + tailLogs(SERVICE_PINOT_CONTROLLER)
                + "\n=== pinot-broker logs ===\n" + tailLogs(SERVICE_PINOT_BROKER)
                + "\n=== pinot-server logs ===\n" + tailLogs(SERVICE_PINOT_SERVER);
    }

    // Reads and truncates service logs to keep assertion output concise.
    private String tailLogs(String serviceName) {
        try {
            String logs = findContainer(serviceName).getLogs();
            String[] lines = logs.split("\\R");
            int from = Math.max(0, lines.length - LOG_TAIL_LINES);
            return String.join(System.lineSeparator(), Arrays.copyOfRange(lines, from, lines.length));
        } catch (Exception e) {
            return "Failed to read logs for " + serviceName + ": " + e.getMessage();
        }
    }

    // Extracts JWT token from login response across supported response shapes.
    private String extractToken(Response loginResponse) {
        String tokenValue = loginResponse.jsonPath().getString("access_token.token");
        if (tokenValue == null || tokenValue.isBlank()) {
            tokenValue = loginResponse.jsonPath().getString("token");
        }
        assertNotNull(tokenValue, "Token not found in login response: " + loginResponse.asString());
        assertTrue(!tokenValue.isBlank(), "Token is blank in login response: " + loginResponse.asString());
        return tokenValue;
    }

    // Converts Pinot response values to long with safe fallback for invalid types.
    private long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException ignored) {
                return -1;
            }
        }
        return -1;
    }
}
