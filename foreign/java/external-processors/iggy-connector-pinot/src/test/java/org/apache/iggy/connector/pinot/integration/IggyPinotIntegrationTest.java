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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
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

import static org.assertj.core.api.Assertions.assertThat;

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
    private static final Duration INGESTION_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration BULK_INGESTION_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int MESSAGES_TO_SEND = 10;
    private static final int BULK_MESSAGES_TO_SEND = 120;
    private static final int BULK_BATCH_SIZE = 20;
    private static final int LOG_TAIL_LINES = 80;
    private static final int REQUEST_MAX_ATTEMPTS = 3;
    private static final Duration REQUEST_RETRY_DELAY = Duration.ofSeconds(2);

    private static String iggyUrl;
    private static String controllerUrl;
    private static String brokerUrl;
    private static String serverUrl;
    private static String token;

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
    static void init() {
        iggyUrl = baseUrl(SERVICE_IGGY, IGGY_HTTP_PORT);
        controllerUrl = baseUrl(SERVICE_PINOT_CONTROLLER, PINOT_CONTROLLER_PORT);
        brokerUrl = baseUrl(SERVICE_PINOT_BROKER, PINOT_BROKER_PORT);
        serverUrl = baseUrl(SERVICE_PINOT_SERVER, PINOT_SERVER_ADMIN_PORT);
    }

    @Test
    @Order(1)
    void step1_servicesAreRunning() {
        System.out.println("Step 1: Checking services");
        System.out.println("Iggy: " + iggyUrl);
        System.out.println("Pinot Controller: " + controllerUrl);
        System.out.println("Pinot Broker: " + brokerUrl);
        System.out.println("Pinot Server: " + serverUrl);
        assertThat(get(iggyUrl + "/").statusCode()).isEqualTo(200);
        assertThat(get(controllerUrl + "/health").statusCode()).isEqualTo(200);
        assertThat(get(brokerUrl + "/health").statusCode()).isEqualTo(200);
        assertThat(get(serverUrl + "/health").statusCode()).isEqualTo(200);
    }

    @Test
    @Order(2)
    void step2_loginToIggy() {
        System.out.println("Step 2: Login to Iggy");
        Response loginResponse = postJson(
                iggyUrl + "/users/login",
                "{\"username\":\"iggy\",\"password\":\"iggy\"}",
                null);
        System.out.println("Login response: " + loginResponse.statusCode());
        System.out.println(loginResponse.asString());
        assertSuccessful(loginResponse, "Login to Iggy");
        token = extractToken(loginResponse);
    }

    @Test
    @Order(3)
    void step3_createStream() {
        System.out.println("Step 3: Create stream");
        Response streamResponse = postJson(
                iggyUrl + "/streams",
                "{\"stream_id\":1,\"name\":\"test-stream\"}",
                token);
        System.out.println("Create stream response: " + streamResponse.statusCode());
        System.out.println(streamResponse.asString());
        assertSuccessful(streamResponse, "Create stream");
    }

    @Test
    @Order(4)
    void step4_createTopic() {
        System.out.println("Step 4: Create topic");
        Response topicResponse = postJson(
                iggyUrl + "/streams/test-stream/topics",
                "{\"topic_id\":1,\"name\":\"test-events\",\"partitions_count\":2,"
                        + "\"compression_algorithm\":\"none\",\"message_expiry\":0,\"max_topic_size\":0}",
                token);
        System.out.println("Create topic response: " + topicResponse.statusCode());
        System.out.println(topicResponse.asString());
        assertSuccessful(topicResponse, "Create topic");
    }

    @Test
    @Order(5)
    void step5_createConsumerGroup() {
        System.out.println("Step 5: Create consumer group");
        Response groupResponse = postJson(
                iggyUrl + "/streams/test-stream/topics/test-events/consumer-groups",
                "{\"name\":\"pinot-integration-test\"}",
                token);
        System.out.println("Create consumer group response: " + groupResponse.statusCode());
        System.out.println(groupResponse.asString());
        assertSuccessful(groupResponse, "Create consumer group");
    }

    @Test
    @Order(6)
    void step6_ensurePinotTenants() throws Exception {
        System.out.println("Step 6: Ensure Pinot tenants");
        assertTenantAdded("BROKER");
        assertTenantAdded("SERVER");
    }

    @Test
    @Order(7)
    void step7_createSchema() throws Exception {
        System.out.println("Step 7: Create schema");
        String schemaJson = Files.readString(resolvePath("deployment/schema.json"), StandardCharsets.UTF_8);
        Response schemaResponse = postJson(controllerUrl + "/schemas", schemaJson, null);
        System.out.println("Create schema response: " + schemaResponse.statusCode());
        System.out.println(schemaResponse.asString());
        assertSuccessful(schemaResponse, "Create schema");
    }

    @Test
    @Order(8)
    void step8_createTable() throws Exception {
        System.out.println("Step 8: Create table");
        String tableJson = Files.readString(resolvePath("deployment/table.json"), StandardCharsets.UTF_8);
        Response tableResponse = postJson(controllerUrl + "/tables", tableJson, null);
        System.out.println("Create table response: " + tableResponse.statusCode());
        System.out.println(tableResponse.asString());
        assertSuccessful(tableResponse, "Create table");
    }

    @Test
    @Order(9)
    void step9_sendMessages() {
        System.out.println("Step 9: Send messages");
        for (int i = 1; i <= MESSAGES_TO_SEND; i++) {
            long timestamp = System.currentTimeMillis();
            String payloadJson = eventPayloadJson("user" + i, "test_event", "desktop", i * 100L, timestamp);
            sendEventPayloads(List.of(payloadJson), "Send message " + i);
        }
    }

    @Test
    @Order(10)
    void step10_pollPinotForIngestion() {
        System.out.println("Step 10: Poll Pinot for ingestion");
        long count = waitForCountAtLeast(MESSAGES_TO_SEND, INGESTION_TIMEOUT);
        assertThat(count)
                .withFailMessage("Expected at least %s rows after initial ingestion, but got %s", MESSAGES_TO_SEND, count)
                .isGreaterThanOrEqualTo(MESSAGES_TO_SEND);
    }

    @Test
    @Order(11)
    void step11_validateJsonParsingAndSchemaMapping() {
        System.out.println("Step 11: Validate JSON parsing and schema mapping");
        String userId = "schema-user-" + System.currentTimeMillis();
        long expectedDuration = 4242L;
        long expectedTimestamp = System.currentTimeMillis();
        String payloadJson = eventPayloadJson(userId, "schema_event", "mobile", expectedDuration, expectedTimestamp);
        sendEventPayloads(List.of(payloadJson), "Send schema mapping test message");

        String sql = "SELECT * FROM test_events_REALTIME WHERE userId = '" + userId + "' LIMIT 1";
        Response response = waitForQueryRow(sql, INGESTION_TIMEOUT);
        List<String> columnNames = response.jsonPath().getList("resultTable.dataSchema.columnNames");
        List<Object> row = response.jsonPath().getList("resultTable.rows[0]");
        int userIdIndex = columnIndex(columnNames, "userId");
        int eventTypeIndex = columnIndex(columnNames, "eventType");
        int deviceTypeIndex = columnIndex(columnNames, "deviceType");
        int durationIndex = columnIndex(columnNames, "duration");
        int timestampIndex = columnIndex(columnNames, "timestamp");

        assertThat(row)
                .withFailMessage("Expected one matching row for userId=%s, response=%s", userId, response.asString())
                .hasSizeGreaterThan(Math.max(timestampIndex, Math.max(userIdIndex, durationIndex)));
        assertThat(String.valueOf(row.get(userIdIndex))).isEqualTo(userId);
        assertThat(String.valueOf(row.get(eventTypeIndex))).isEqualTo("schema_event");
        assertThat(String.valueOf(row.get(deviceTypeIndex))).isEqualTo("mobile");
        assertThat(toLong(row.get(durationIndex))).isEqualTo(expectedDuration);
        assertThat(toLong(row.get(timestampIndex))).isEqualTo(expectedTimestamp);
    }

    @Test
    @Order(12)
    void step12_bulkMessageIngestion() {
        System.out.println("Step 12: Bulk message ingestion");
        long baselineCount = fetchCurrentCount();
        String userPrefix = "bulk-user-" + System.currentTimeMillis() + "-";

        int sent = 0;
        while (sent < BULK_MESSAGES_TO_SEND) {
            int batchSize = Math.min(BULK_BATCH_SIZE, BULK_MESSAGES_TO_SEND - sent);
            List<String> batchPayloads = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                int index = sent + i;
                batchPayloads.add(
                        eventPayloadJson(
                                userPrefix + index,
                                "bulk_event",
                                "mobile",
                                1000L + index,
                                System.currentTimeMillis()));
            }
            int batchNumber = (sent / BULK_BATCH_SIZE) + 1;
            sendEventPayloads(batchPayloads, "Send bulk batch " + batchNumber + " (" + batchSize + " messages)");
            sent += batchSize;
        }

        long expectedCount = baselineCount + BULK_MESSAGES_TO_SEND;
        long finalCount = waitForCountAtLeast(expectedCount, BULK_INGESTION_TIMEOUT);
        assertThat(finalCount)
                .withFailMessage(
                        "Bulk ingestion expected at least %s rows, baseline=%s, final=%s",
                        expectedCount,
                        baselineCount,
                        finalCount)
                .isGreaterThanOrEqualTo(expectedCount);
    }

    private static String baseUrl(String service, int port) {
        String host = environment.getServiceHost(service, port);
        Integer mappedPort = environment.getServicePort(service, port);
        return "http://" + host + ":" + mappedPort;
    }

    private static Response get(String url) {
        return RestAssured.given()
                .accept("*/*")
                .when()
                .get(url)
                .andReturn();
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

    private static Response postJson(String url, String json, String token) {
        for (int attempt = 1; attempt <= REQUEST_MAX_ATTEMPTS; attempt++) {
            try {
                io.restassured.specification.RequestSpecification request = RestAssured.given()
                        .accept("*/*")
                        .contentType("application/json")
                        .body(json);
                if (token != null) {
                    request.header("Authorization", "Bearer " + token);
                }
                Response response = request
                        .when()
                        .post(url)
                        .andReturn();
                if (response.statusCode() >= 200 && response.statusCode() <= 300) {
                    return response;
                }
                if (attempt == REQUEST_MAX_ATTEMPTS) {
                    return response;
                }
                System.out.println("Request failed with status " + response.statusCode() + ", retrying...");
            } catch (RuntimeException e) {
                if (attempt == REQUEST_MAX_ATTEMPTS) {
                    throw e;
                }
                System.out.println("Request error: " + e.getMessage() + ", retrying...");
            }
            sleepWithInterruptHandling(REQUEST_RETRY_DELAY.toMillis());
        }
        throw new IllegalStateException("Request retries exhausted");
    }

    private static void sleepWithInterruptHandling(long sleepMillis) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while waiting", e);
        }
    }

    private static void assertSuccessful(Response response, String action) {
        assertThat(response.statusCode())
                .withFailMessage("%s failed, status=%s, body=%s", action, response.statusCode(), response.asString())
                .isBetween(200, 299);
    }

    private static long waitForCountAtLeast(long expectedCount, Duration timeout) {
        String queryBody = "{\"sql\":\"SELECT COUNT(*) FROM test_events_REALTIME\"}";
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long count = -1;
        int lastStatus = -1;
        String lastQueryResponse = "";

        while (System.currentTimeMillis() < deadline) {
            Response response = postJson(brokerUrl + "/query/sql", queryBody, null);
            lastStatus = response.statusCode();
            lastQueryResponse = response.asString();
            System.out.println("Count query response: " + lastStatus);
            System.out.println(lastQueryResponse);
            if (lastStatus >= 200 && lastStatus < 300) {
                count = extractCount(response);
                if (count >= expectedCount) {
                    return count;
                }
            }
            sleepWithInterruptHandling(POLL_INTERVAL.toMillis());
        }

        throw new AssertionError(
                "Expected at least " + expectedCount + " ingested rows, but got " + count
                        + ", lastStatus=" + lastStatus
                        + ", lastQueryResponse=" + lastQueryResponse
                        + System.lineSeparator()
                        + diagnosticLogs());
    }

    private static long fetchCurrentCount() {
        String queryBody = "{\"sql\":\"SELECT COUNT(*) FROM test_events_REALTIME\"}";
        Response response = postJson(brokerUrl + "/query/sql", queryBody, null);
        assertSuccessful(response, "Fetch current row count");
        return extractCount(response);
    }

    private static Response waitForQueryRow(String sql, Duration timeout) {
        String queryBody = queryBody(sql);
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
            sleepWithInterruptHandling(POLL_INTERVAL.toMillis());
        }

        throw new AssertionError(
                "Query did not return rows in time. sql=" + sql
                        + ", lastStatus=" + lastStatus
                        + ", lastBody=" + lastBody
                        + System.lineSeparator()
                        + diagnosticLogs());
    }

    private static int columnIndex(List<String> columnNames, String columnName) {
        assertThat(columnNames)
                .withFailMessage("Column names missing in query response while resolving %s", columnName)
                .isNotNull();
        int index = columnNames.indexOf(columnName);
        assertThat(index)
                .withFailMessage("Column %s not found in Pinot response columns=%s", columnName, columnNames)
                .isGreaterThanOrEqualTo(0);
        return index;
    }

    private static String queryBody(String sql) {
        return "{\"sql\":\"" + escapeJson(sql) + "\"}";
    }

    private static String escapeJson(String raw) {
        return raw.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    private static String eventPayloadJson(
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

    private static void sendEventPayloads(List<String> payloadJsonMessages, String action) {
        String partitionValue = Base64.getEncoder().encodeToString(new byte[] {0, 0, 0, 0});
        StringBuilder messages = new StringBuilder();
        for (int i = 0; i < payloadJsonMessages.size(); i++) {
            if (i > 0) {
                messages.append(",");
            }
            String payload = Base64.getEncoder()
                    .encodeToString(payloadJsonMessages.get(i).getBytes(StandardCharsets.UTF_8));
            messages.append("{\"payload\":\"").append(payload).append("\"}");
        }

        String body = "{\"partitioning\":{\"kind\":\"partition_id\",\"value\":\"" + partitionValue + "\"},"
                + "\"messages\":[" + messages + "]}";
        Response sendResponse = postJson(
                iggyUrl + "/streams/test-stream/topics/test-events/messages",
                body,
                token);
        System.out.println(action + " response: " + sendResponse.statusCode());
        System.out.println(sendResponse.asString());
        assertSuccessful(sendResponse, action);
    }

    private static void assertTenantAdded(String role) throws Exception {
        org.testcontainers.containers.Container.ExecResult result = findContainer(SERVICE_PINOT_CONTROLLER)
                .execInContainer(
                        "bin/pinot-admin.sh",
                        "AddTenant",
                        "-name", "DefaultTenant",
                        "-role", role,
                        "-instanceCount", "1",
                        "-controllerHost", "localhost",
                        "-controllerPort", "9000");
        assertThat(result.getExitCode())
                .withFailMessage(
                        "AddTenant failed for role=%s%nstdout:%n%s%nstderr:%n%s",
                        role,
                        result.getStdout(),
                        result.getStderr())
                .isEqualTo(0);
    }

    private static ContainerState findContainer(String serviceName) {
        Optional<ContainerState> byInstance = environment.getContainerByServiceName(serviceName + "_1");
        if (byInstance.isPresent()) {
            return byInstance.get();
        }
        return environment.getContainerByServiceName(serviceName)
                .orElseThrow(() -> new IllegalStateException(
                        "Container not found for service=" + serviceName));
    }

    private static String diagnosticLogs() {
        return "=== iggy logs ===\n" + tailLogs(SERVICE_IGGY)
                + "\n=== pinot-controller logs ===\n" + tailLogs(SERVICE_PINOT_CONTROLLER)
                + "\n=== pinot-broker logs ===\n" + tailLogs(SERVICE_PINOT_BROKER)
                + "\n=== pinot-server logs ===\n" + tailLogs(SERVICE_PINOT_SERVER);
    }

    private static String tailLogs(String serviceName) {
        try {
            String logs = findContainer(serviceName).getLogs();
            return tail(logs, LOG_TAIL_LINES);
        } catch (Exception e) {
            return "Failed to read logs for " + serviceName + ": " + e.getMessage();
        }
    }

    private static String tail(String logs, int maxLines) {
        String[] lines = logs.split("\\R");
        int from = Math.max(0, lines.length - maxLines);
        return String.join(System.lineSeparator(), Arrays.copyOfRange(lines, from, lines.length));
    }

    private static String extractToken(Response loginResponse) {
        String extracted = loginResponse.jsonPath().getString("access_token.token");
        if (extracted == null || extracted.isBlank()) {
            extracted = loginResponse.jsonPath().getString("token");
        }
        assertThat(extracted)
                .withFailMessage("Token not found in login response: %s", loginResponse.asString())
                .isNotBlank();
        return extracted;
    }

    private static long extractCount(Response response) {
        Object value = response.jsonPath().get("resultTable.rows[0][0]");
        return toLong(value);
    }

    private static long toLong(Object value) {
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
