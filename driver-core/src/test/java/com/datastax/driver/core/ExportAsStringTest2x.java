/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ExportAsStringTest.getExpectedCqlString;
import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.0")
public class ExportAsStringTest2x extends CCMTestsSupport {

    /**
     * A version of {@link ExportAsStringTest} for C* 2.0 and 2.1 clusters.
     */
    @Test(groups = "short")
    public void create_schema_and_ensure_exported_cql_is_expected() {
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.2")) >= 0) {
            throw new SkipException("This test is only meant for C* 2.0-2.1");
        }

        String keyspace = "complex_ks";
        Map<String, Object> replicationOptions = ImmutableMap.<String, Object>of("class", "SimpleStrategy", "replication_factor", 1);

        // create keyspace
        session().execute(SchemaBuilder.createKeyspace(keyspace).with().replication(replicationOptions));

        // create session from this keyspace.
        Session session = cluster().connect(keyspace);

        KeyspaceMetadata ks;

        // udts require 2.1+
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.1")) >= 0) {
            // Usertype 'ztype' with two columns.  Given name to ensure that even though it has an alphabetically
            // later name, it shows up before other user types ('ctype') that depend on it.
            session.execute(SchemaBuilder.createType("ztype")
                    .addColumn("c", DataType.text()).addColumn("a", DataType.cint()));

            // Usertype 'xtype' with two columns.  At same level as 'ztype' since both are depended on by ctype, should
            // show up before 'ztype' because it's alphabetically before, even though it was created after.
            session.execute(SchemaBuilder.createType("xtype")
                    .addColumn("d", DataType.text()));

            // Refetch keyspace for < 3.0 schema this is required as a new keyspace metadata reference may be created.
            ks = cluster().getMetadata().getKeyspace(keyspace);

            // Usertype 'ctype' which depends on both ztype and xtype, therefore ztype and xtype should show up earlier.
            session.execute(SchemaBuilder.createType("ctype")
                    .addColumn("z", ks.getUserType("ztype").copy(true))
                    .addColumn("x", ks.getUserType("xtype").copy(true)));

            // Usertype 'btype' which has no dependencies, should show up before 'xtype' and 'ztype' since it's
            // alphabetically before.
            session.execute(SchemaBuilder.createType("btype").addColumn("a", DataType.text()));

            ks = cluster().getMetadata().getKeyspace(keyspace);

            // Usertype 'atype' which depends on 'ctype', so should show up after 'ctype', 'xtype' and 'ztype'.
            session.execute(SchemaBuilder.createType("atype")
                    .addColumn("c", ks.getUserType("ctype").copy(true)));
        }

        // A simple table with LCS compaction strategy.
        session.execute(SchemaBuilder.createTable("ztable")
                .addPartitionKey("zkey", DataType.text())
                .addColumn("a", DataType.cint())
                .withOptions().compactionOptions(SchemaBuilder.leveledStrategy().ssTableSizeInMB(95)));

        // A table with a secondary index, taken from documentation on secondary index.
        session.execute(SchemaBuilder.createTable("rank_by_year_and_name")
                .addPartitionKey("race_year", DataType.cint())
                .addPartitionKey("race_name", DataType.text())
                .addClusteringColumn("rank", DataType.cint())
                .addColumn("cyclist_name", DataType.text()));

        session.execute(SchemaBuilder.createIndex("ryear")
                .onTable("rank_by_year_and_name")
                .andColumn("race_year"));

        session.execute(SchemaBuilder.createIndex("rrank")
                .onTable("rank_by_year_and_name")
                .andColumn("rank"));

        ks = cluster().getMetadata().getKeyspace(keyspace);
        assertThat(ks.exportAsString().trim()).isEqualTo(getExpectedCqlString(ccm().getCassandraVersion()));
    }
}
