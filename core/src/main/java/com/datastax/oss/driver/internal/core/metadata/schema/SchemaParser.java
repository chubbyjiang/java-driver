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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.SchemaElementKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes the rows that were read from system tables and prepares a refresh of the corresponding
 * metadata.
 */
public class SchemaParser {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaParser.class);

  private final SchemaRows rows;
  private final DefaultMetadata currentMetadata;
  private final InternalDriverContext context;
  private final String logPrefix;
  private final DataTypeParser dataTypeParser;

  public SchemaParser(
      SchemaRows rows,
      DefaultMetadata currentMetadata,
      InternalDriverContext context,
      String logPrefix) {
    this.rows = rows;
    this.currentMetadata = currentMetadata;
    this.context = context;
    this.logPrefix = logPrefix;

    CassandraVersion version = rows.node.getCassandraVersion().nextStable();
    this.dataTypeParser =
        version.compareTo(CassandraVersion.V3_0_0) < 0
            ? new DataTypeClassNameParser()
            : new DataTypeCqlNameParser();
  }

  /**
   * @return the refresh, or {@code null} if it could not be computed (most likely due to malformed
   *     system rows).
   */
  public MetadataRefresh parse() {
    switch (rows.refreshKind) {
      case FULL_SCHEMA:
        break;
      case KEYSPACE:
        break;
      case TABLE:
        break;
      case VIEW:
        break;
      case TYPE:
        break;
      case FUNCTION:
        return parseSingleRow(rows.functions, rows.refreshKind, this::parseFunction);
      case AGGREGATE:
        return parseSingleRow(rows.aggregates, rows.refreshKind, this::parseAggregate);
      default:
        throw new AssertionError("Unsupported schema refresh kind " + this.rows.refreshKind);
    }
    throw new UnsupportedOperationException("TODO");
  }

  private MetadataRefresh parseSingleRow(
      Multimap<CqlIdentifier, AdminResult.Row> rows, SchemaElementKind kind, RowParser rowParser) {
    if (rows.size() != 1) {
      LOG.warn(
          "[{}] Processing a {} refresh, expected a single row but found {}, skipping",
          logPrefix,
          kind,
          rows.size());
      return null;
    } else {
      Map.Entry<CqlIdentifier, AdminResult.Row> entry = rows.entries().iterator().next();
      CqlIdentifier keyspaceId = entry.getKey();
      AdminResult.Row row = entry.getValue();
      // Since we're refreshing a single element, we know that any UDT it depends on is already
      // present in the keyspace.
      KeyspaceMetadata keyspace = currentMetadata.getKeyspaces().get(keyspaceId);
      if (keyspace == null) {
        LOG.warn(
            "[{}] Processing a {} refresh for {}.{} " + "but that keyspace is unknown, skipping",
            logPrefix,
            kind,
            keyspaceId,
            row.getString(kind.name().toLowerCase() + "_name"));
        return null;
      }
      return rowParser.parse(row, keyspaceId, keyspace.getUserDefinedTypes());
    }
  }

  interface RowParser {
    MetadataRefresh parse(
        AdminResult.Row row,
        CqlIdentifier keyspaceId,
        Map<CqlIdentifier, UserDefinedType> userDefinedTypes);
  }

  private MetadataRefresh parseFunction(
      AdminResult.Row row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_functions (
    //     keyspace_name text,
    //     function_name text,
    //     signature frozen<list<text>>,
    //     argument_names list<text>,
    //     argument_types list<text>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, signature)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, signature ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.functions (
    //     keyspace_name text,
    //     function_name text,
    //     argument_names frozen<list<text>>,
    //     argument_types frozen<list<text>>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, argument_types ASC)
    String simpleName = row.getString("function_name");
    List<CqlIdentifier> argumentNames = toCqlIdentifiers(row.getListOfString("argument_names"));
    List<String> argumentTypes = row.getListOfString("argument_types");
    if (argumentNames.size() != argumentTypes.size()) {
      LOG.warn(
          "[{}] Error parsing system row for function {}.{}, "
              + "number of argument names and types don't match (got {} and {}).",
          logPrefix,
          keyspaceId.asInternal(),
          simpleName,
          argumentNames.size(),
          argumentTypes.size());
      return null;
    }
    FunctionSignature signature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(simpleName),
            parseTypes(argumentTypes, keyspaceId, userDefinedTypes));
    String body = row.getString("body");
    Boolean calledOnNullInput = row.getBoolean("called_on_null_input");
    String language = row.getString("language");
    DataType returnType = parseType(row.getString("return_type"), keyspaceId, userDefinedTypes);

    return new FunctionRefresh(
        currentMetadata,
        new DefaultFunctionMetadata(
            keyspaceId, signature, argumentNames, body, calledOnNullInput, language, returnType),
        logPrefix);
  }

  private MetadataRefresh parseAggregate(
      AdminResult.Row row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     signature frozen<list<text>>,
    //     argument_types list<text>,
    //     final_func text,
    //     initcond blob,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, signature)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, signature ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     argument_types frozen<list<text>>,
    //     final_func text,
    //     initcond text,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, argument_types ASC)
    String simpleName = row.getString("aggregate_name");
    List<String> argumentTypes = row.getListOfString("argument_types");
    FunctionSignature signature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(simpleName),
            parseTypes(argumentTypes, keyspaceId, userDefinedTypes));

    DataType stateType = parseType(row.getString("state_type"), keyspaceId, userDefinedTypes);
    TypeCodec<Object> stateTypeCodec = context.codecRegistry().codecFor(stateType);

    String stateFuncSimpleName = row.getString("state_func");
    FunctionSignature stateFuncSignature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(stateFuncSimpleName),
            ImmutableList.<DataType>builder()
                .add(stateType)
                .addAll(signature.getParameterTypes())
                .build());

    String finalFuncSimpleName = row.getString("final_func");
    FunctionSignature finalFuncSignature =
        (finalFuncSimpleName == null)
            ? null
            : new FunctionSignature(CqlIdentifier.fromInternal(finalFuncSimpleName), stateType);

    DataType returnType = parseType(row.getString("return_type"), keyspaceId, userDefinedTypes);

    Object initCond;
    if (row.isString("initcond")) { // Cassandra 3
      String initCondString = row.getString("initcond");
      initCond = (initCondString == null) ? null : stateTypeCodec.parse(initCondString);
    } else { // Cassandra 2.2
      ByteBuffer initCondBlob = row.getByteBuffer("initcond");
      initCond =
          (initCondBlob == null)
              ? null
              : stateTypeCodec.decode(initCondBlob, context.protocolVersion());
    }
    return new AggregateRefresh(
        currentMetadata,
        new DefaultAggregateMetadata(
            keyspaceId,
            signature,
            finalFuncSignature,
            initCond,
            returnType,
            stateFuncSignature,
            stateType,
            stateTypeCodec),
        logPrefix);
  }

  private DataType parseType(
      String typeString, CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> userTypes) {
    return dataTypeParser.parse(typeString, keyspaceId, userTypes, context);
  }

  private List<DataType> parseTypes(
      List<String> typeStrings,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userTypes) {
    if (typeStrings.isEmpty()) {
      return Collections.emptyList();
    } else {
      ImmutableList.Builder<DataType> builder = ImmutableList.builder();
      for (String typeString : typeStrings) {
        builder.add(parseType(typeString, keyspaceId, userTypes));
      }
      return builder.build();
    }
  }

  private List<CqlIdentifier> toCqlIdentifiers(List<String> internalNames) {
    if (internalNames.isEmpty()) {
      return Collections.emptyList();
    } else {
      ImmutableList.Builder<CqlIdentifier> builder = ImmutableList.builder();
      for (String name : internalNames) {
        builder.add(CqlIdentifier.fromInternal(name));
      }
      return builder.build();
    }
  }
}
