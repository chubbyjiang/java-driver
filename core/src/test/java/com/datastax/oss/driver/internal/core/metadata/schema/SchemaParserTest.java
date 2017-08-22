package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class SchemaParserTest {

  @Mock protected DefaultMetadata currentMetadata;
  @Mock protected InternalDriverContext context;
  @Mock protected Node node;

  @Before
  public void setup() {
    Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.V3_0_0);
  }

  protected static AdminResult.Row mockFunctionRow(
      String keyspace, String name, CassandraVersion cassandraVersion) {

    AdminResult.Row row = Mockito.mock(AdminResult.Row.class);

    String intType =
        (cassandraVersion.compareTo(CassandraVersion.V3_0_0) < 0)
            ? "org.apache.cassandra.db.marshal.Int32Type"
            : "int";

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("function_name")).thenReturn(name);
    Mockito.when(row.getListOfString("argument_names")).thenReturn(ImmutableList.of("i"));
    Mockito.when(row.getListOfString("argument_types")).thenReturn(ImmutableList.of(intType));
    Mockito.when(row.getString("body")).thenReturn("return i;");
    Mockito.when(row.getBoolean("called_on_null_input")).thenReturn(false);
    Mockito.when(row.getString("language")).thenReturn("java");
    Mockito.when(row.getString("return_type")).thenReturn(intType);

    return row;
  }
}
