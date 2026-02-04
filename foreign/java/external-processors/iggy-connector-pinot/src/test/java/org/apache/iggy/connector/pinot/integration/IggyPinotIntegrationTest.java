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
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class IggyPinotIntegrationTest {

    private static final int IGGY_HTTP_PORT = 3000;
    private static final int PINOT_CONTROLLER_PORT = 9000;
    private static final int PINOT_BROKER_PORT = 8099;
    private static final int PINOT_SERVER_ADMIN_PORT = 8097;

    @Container
    static final DockerComposeContainer<?> environment =
            new DockerComposeContainer<>(new File("docker-compose.yml"))
                    .withExposedService(
                            "iggy",
                            IGGY_HTTP_PORT,
                            Wait.forHttp("/").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-controller",
                            PINOT_CONTROLLER_PORT,
                            Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-broker",
                            PINOT_BROKER_PORT,
                            Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService(
                            "pinot-server",
                            PINOT_SERVER_ADMIN_PORT,
                            Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                    .withLocalCompose(true);

    @Test
    void servicesAreRunning() {
        assertThat(environment.isRunning()).isTrue();
    }

    static String iggyHttpBaseUrl() {
        return baseUrl("iggy", IGGY_HTTP_PORT);
    }

    static String pinotControllerBaseUrl() {
        return baseUrl("pinot-controller", PINOT_CONTROLLER_PORT);
    }

    static String pinotBrokerBaseUrl() {
        return baseUrl("pinot-broker", PINOT_BROKER_PORT);
    }

    static String pinotServerAdminBaseUrl() {
        return baseUrl("pinot-server", PINOT_SERVER_ADMIN_PORT);
    }

    private static String baseUrl(String service, int port) {
        String host = environment.getServiceHost(service, port);
        Integer mappedPort = environment.getServicePort(service, port);
        return "http://" + host + ":" + mappedPort;
    }
}
