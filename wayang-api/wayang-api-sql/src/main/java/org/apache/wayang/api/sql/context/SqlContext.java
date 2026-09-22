/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.context;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.lang3.StringUtils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.api.utils.Parameters;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;

import scala.collection.JavaConversions;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class SqlContext extends WayangContext {

    private static final AtomicInteger jobId = new AtomicInteger(0);

    private final CalciteSchema calciteSchema;

    public SqlContext() throws SQLException {
        this(new Configuration());
    }

    public SqlContext(final Configuration configuration) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        this.withPlugin(Java.basicPlugin());
        this.withPlugin(Spark.basicPlugin());
        this.withPlugin(Postgres.plugin());

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(final Configuration configuration, final List<Plugin> plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (final Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    /**
     * Entry point for executing SQL statements while providing arguments.
     * You need to provide at least a JDBC source.
     *
     * @param args
     *             <ul>
     *             <li><b>-p, --platforms</b>: Comma-separated list of execution
     *             platforms (e.g., spark, java).</li>
     *             <li><b>-q, --query</b>: Path to the SQL query file to be
     *             executed.</li>
     *             <li><b>-o, --outputPath</b>: Path where the output results will
     *             be stored.</li>
     *             <li><b>-c, --config</b>: Path to the configuration file.</li>
     *             </ul>
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 4)
            throw new IllegalArgumentException(
                    "Usage: ./bin/wayang-submit org.apache.wayang.api.sql.SqlContext <configuration path> <SQL statement path> <output path> [platforms...]");

        // Specify the named arguments
        final Options options = new Options();
        options.addOption("p", "platforms", true, "[platforms...]");
        options.addOption("q", "query", true, "SQL statement path");
        options.addOption("o", "outputPath", true, "Output path");
        options.addOption("c", "config", true, "File path for config file");

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final String queryPath = cmd.getOptionValue("q");
        final String outputPath = cmd.getOptionValue("o");

        final String query = StringUtils.chop(Files.readString(Paths.get(queryPath)).stripTrailing());
        final Configuration configuration = new Configuration(cmd.getOptionValue("c"));

        final SqlContext context = new SqlContext(configuration,
                List.of(Java.channelConversionPlugin(), Postgres.conversionPlugin()));

        final List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(cmd.getOptionValue("p")));
        plugins.stream().forEach(context::register);

        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(context.calciteSchema, configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(query);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        PrintUtils.print("After parsing sql query", relNode);

        final RuleSet rules = RuleSets.ofList(
                SubQueryRemoveRule.Config.FILTER.toRule(),
                SubQueryRemoveRule.Config.JOIN.toRule(),
                SubQueryRemoveRule.Config.PROJECT.toRule(),
                CoreRules.FILTER_INTO_JOIN,
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_SORT_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        PrintUtils.print("After translating logical intermediate plan", wayangRel);

        final Collection<Record> collector = new ArrayList<>();
        final WayangPlan wayangPlan = Optimizer.convertWithConfig(wayangRel, configuration, collector);
        collector.add(new Record(wayangRel.getRowType().getFieldNames().toArray()));
        context.execute(getJobName(), wayangPlan);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputPath))) {
            for (final Record rec : collector) {
                writer.write(Arrays.toString(rec.getValues()));
                writer.newLine();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public Collection<Record> executeSql(final String sql) throws SqlParseException {
        return this.executeSqlWithMetadata(sql).getRows();
    }

    public SqlQueryResult executeSqlWithMetadata(final String sql) throws SqlParseException {
        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        PrintUtils.print("After parsing sql query", relNode);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                createDefaultRuleSet());

        PrintUtils.print("After translating logical intermediate plan", wayangRel);

        final Collection<Record> collector = new ArrayList<>();
        final WayangPlan wayangPlan = Optimizer.convertWithConfig(wayangRel, this.getConfiguration(), collector);

        this.execute(getJobName(), wayangPlan);

        return new SqlQueryResult(createColumns(wayangRel.getRowType()), collector);
    }

    /**
     * Creates a metadata snapshot from the same Calcite root schema that is
     * used to validate and execute SQL queries.
     *
     * <p>JDBC exposes schemas as single identifiers, whereas Calcite schemas
     * can be nested. Therefore, this Phase 1 snapshot includes the root
     * schema's tables and each immediate child schema's direct tables.
     * Deeper nested schemas are deliberately omitted instead of publishing a
     * dotted name that JDBC clients would quote as one, incorrect identifier.</p>
     *
     * @return configured schemas, tables, and table row types
     * @throws SQLException if a schema or table cannot expose its metadata
     */
    public SqlCatalogMetadata getCatalogMetadata() throws SQLException {
        try {
            final SchemaPlus rootSchema = this.calciteSchema.plus();
            final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
            final List<SqlSchemaMetadata> schemas = new ArrayList<>();

            if (!rootSchema.getTableNames().isEmpty()) {
                schemas.add(this.createSchemaMetadata(rootSchema, "", typeFactory));
            }

            final List<String> schemaNames = new ArrayList<>(rootSchema.getSubSchemaNames());
            schemaNames.sort(Comparator.naturalOrder());
            for (final String schemaName : schemaNames) {
                final SchemaPlus schema = rootSchema.getSubSchema(schemaName);
                if (schema != null) {
                    schemas.add(this.createSchemaMetadata(schema, schemaName, typeFactory));
                }
            }
            return new SqlCatalogMetadata(schemas);
        } catch (final RuntimeException e) {
            final SQLException sqlException = findSqlException(e);
            if (sqlException != null) {
                throw sqlException;
            }
            throw new SQLException(
                    "Could not inspect the configured Calcite catalog.",
                    "HY000",
                    e
            );
        }
    }

    private static RuleSet createDefaultRuleSet() {
        return RuleSets.ofList(
                SubQueryRemoveRule.Config.FILTER.toRule(),
                SubQueryRemoveRule.Config.JOIN.toRule(),
                SubQueryRemoveRule.Config.PROJECT.toRule(),
                CoreRules.FILTER_INTO_JOIN,
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_SORT_RULE);
    }

    private static List<SqlColumn> createColumns(final RelDataType rowType) {
        final List<SqlColumn> columns = new ArrayList<>(rowType.getFieldCount());
        for (final RelDataTypeField field : rowType.getFieldList()) {
            final RelDataType fieldType = field.getType();
            final SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
            columns.add(new SqlColumn(
                    field.getName(),
                    field.getName(),
                    sqlTypeName.getName(),
                    sqlTypeName.getJdbcOrdinal(),
                    Math.max(0, fieldType.getPrecision()),
                    Math.max(0, fieldType.getScale()),
                    fieldType.isNullable()
            ));
        }
        return columns;
    }

    private SqlSchemaMetadata createSchemaMetadata(
            final SchemaPlus schema,
            final String schemaName,
            final RelDataTypeFactory typeFactory
    ) throws SQLException {
        final List<String> tableNames = new ArrayList<>(schema.getTableNames());
        tableNames.sort(Comparator.naturalOrder());
        final List<SqlTableMetadata> tables = new ArrayList<>(tableNames.size());
        try {
            for (final String tableName : tableNames) {
                final Table table = schema.getTable(tableName);
                if (table == null) {
                    continue;
                }
                final Schema.TableType tableType = table.getJdbcTableType();
                tables.add(new SqlTableMetadata(
                        tableName,
                        tableType == null ? Schema.TableType.TABLE.jdbcName : tableType.jdbcName,
                        createColumns(table.getRowType(typeFactory))
                ));
            }
        } catch (final RuntimeException e) {
            final SQLException sqlException = findSqlException(e);
            if (sqlException != null) {
                throw sqlException;
            }
            throw new SQLException(
                    "Could not inspect Calcite schema '" + schemaName + "'.",
                    "HY000",
                    e
            );
        }
        return new SqlSchemaMetadata(schemaName, tables);
    }

    private static SQLException findSqlException(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException) {
                return (SQLException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    private static String getJobName() {
        return "SQL[" + jobId.incrementAndGet() + "]";
    }

}
