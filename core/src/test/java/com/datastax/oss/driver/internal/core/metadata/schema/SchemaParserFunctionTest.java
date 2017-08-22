/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.SchemaElementKind;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserFunctionTest extends SchemaParserTest {

  @Test
  public void should_skip_when_no_rows() {
    assertThat(parse(/*no rows*/ )).isNull();
  }

  @Test
  public void should_skip_when_keyspace_unknown() {
    assertThat(parse(mockFunctionRow("ks", "id", CassandraVersion.V3_0_0))).isNull();
  }

  @Test
  public void should_parse_modern_table() {
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    FunctionRefresh refresh =
        (FunctionRefresh) parse(mockFunctionRow("ks", "id", CassandraVersion.V3_0_0));

    FunctionMetadata function = refresh.newFunction;
    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }

  @Test
  public void should_parse_legacy_table() {
    Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.V2_2_0);

    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    FunctionRefresh refresh =
        (FunctionRefresh) parse(mockFunctionRow("ks", "id", CassandraVersion.V2_2_0));

    FunctionMetadata function = refresh.newFunction;
    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }

  private MetadataRefresh parse(AdminResult.Row... functionRows) {
    SchemaRows rows =
        new SchemaRows.Builder(node, SchemaElementKind.FUNCTION, "table_name", "test")
            .withFunctions(Arrays.asList(functionRows))
            .build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
