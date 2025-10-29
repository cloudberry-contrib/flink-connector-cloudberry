/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.cloudberry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Example test class demonstrating JUnit 5 and AssertJ usage.
 *
 * <p>This is a template for writing tests with the new testing framework.
 */
@DisplayName("Example Test Suite")
class ExampleTest {

  @BeforeEach
  void setUp() {
    // Setup code runs before each test method
    // This replaces JUnit 4's @Before
  }

  @AfterEach
  void tearDown() {
    // Cleanup code runs after each test method
    // This replaces JUnit 4's @After
  }

  @Test
  @DisplayName("Should demonstrate basic assertions")
  void testBasicAssertions() {
    // AssertJ provides fluent, readable assertions
    String actual = "Hello Cloudberry";

    assertThat(actual)
        .isNotNull()
        .isNotEmpty()
        .startsWith("Hello")
        .endsWith("Cloudberry")
        .contains("Cloud");
  }

  @Test
  @DisplayName("Should demonstrate collection assertions")
  void testCollectionAssertions() {
    java.util.List<String> list = java.util.Arrays.asList("Flink", "Cloudberry", "Connector");

    assertThat(list)
        .isNotEmpty()
        .hasSize(3)
        .contains("Flink", "Cloudberry")
        .doesNotContain("MongoDB");
  }

  @Test
  @DisplayName("Should demonstrate exception assertions")
  void testExceptionHandling() {
    assertThat(
            org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                  throw new IllegalArgumentException("Test exception");
                }))
        .hasMessage("Test exception");
  }
}
