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

package org.apache.wayang.jdbc.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC implementation for {@link ParquetSource}s exposed by an engine as a SQL
 * relation, such as a Hive/Iceberg table, BigQuery external table, or DuckDB
 * view over {@code read_parquet(...)}.
 */
public abstract class JdbcParquetSource extends ParquetSource implements JdbcSourceOperator {

    private static final Pattern PROPERTY_PLACEHOLDER = Pattern.compile("\\$\\{(env|sys):([^}]+)}");

    public JdbcParquetSource(String sourceName, String[] projection, String... columnNames) {
        super(sourceName, projection, columnNames);
    }

    public JdbcParquetSource(ParquetSource that) {
        super(that);
    }

    @Override
    public String getSourceName() {
        return this.getInputUrl();
    }

    @Override
    public String getSourceName(Configuration configuration) {
        return this.resolveSourceName(configuration);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getSourceName();
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler, Configuration configuration) {
        return this.resolveSourceName(connection, configuration);
    }

    @Override
    public void prepareSource(Connection connection, FunctionCompiler compiler, Configuration configuration) {
        this.resolveSourceName(connection, configuration);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.parquetsource.load", this.getPlatform().getPlatformId());
    }

    @Override
    public org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(OptimizationContext optimizationContext,
                                                CardinalityEstimate... inputEstimates) {
                final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                        "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
                );

                try (Connection connection = JdbcParquetSource.this.getPlatform()
                        .createDatabaseDescriptor(optimizationContext.getConfiguration())
                        .createJdbcConnection()) {
                    final String sql = String.format("SELECT count(*) FROM %s",
                            JdbcParquetSource.this.resolveSourceName(
                                    connection,
                                    optimizationContext.getConfiguration()
                            ));
                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);
                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);
                } catch (Exception e) {
                    LogManager.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", JdbcParquetSource.this, e
                    );
                    return new CardinalityEstimate(10, 10000000, 0.9);
                } finally {
                    timeMeasurement.stop();
                }
            }
        };
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            int outputIndex,
            Configuration configuration) {
        return Optional.of(this.getCardinalityEstimator(outputIndex));
    }

    private String resolveSourceName(Configuration configuration) {
        if (configuration == null) {
            return this.getSourceName();
        }

        final String platformId = this.getPlatform().getPlatformId();
        return resolveSourceName(configuration, platformId, this.getInputUrl());
    }

    private String resolveSourceName(Connection connection, Configuration configuration) {
        final String sourceName = this.resolveSourceName(configuration);
        if (configuration == null) {
            return sourceName;
        }

        final String platformId = this.getPlatform().getPlatformId();
        this.executePrepareSql(connection, configuration, platformId);
        if (isAutoCreateEnabled(configuration, platformId)) {
            this.createExternalRelation(connection, configuration, platformId, sourceName);
        }

        return sourceName;
    }

    public static String resolveSourceName(Configuration configuration, String platformId, String inputUrl) {
        if (configuration == null) {
            return inputUrl;
        }

        return findMappedRelation(configuration, platformId, inputUrl)
                .orElseGet(() -> isAutoCreateEnabled(configuration, platformId) && isParquetLocation(inputUrl)
                        ? createGeneratedRelationName(configuration, platformId, inputUrl)
                        : inputUrl);
    }

    private static boolean isParquetLocation(String inputUrl) {
        return inputUrl.contains("://")
                || inputUrl.startsWith("/")
                || inputUrl.startsWith("\\")
                || inputUrl.matches("^[A-Za-z]:[\\\\/].*")
                || inputUrl.contains(".parquet");
    }

    private static Optional<String> findMappedRelation(Configuration configuration, String platformId, String inputUrl) {
        final String mappingKey = String.format("wayang.%s.parquetsource.mappings", platformId);
        final Optional<String> mapping = configuration.getOptionalStringProperty(mappingKey);
        if (mapping.isEmpty()) {
            return Optional.empty();
        }

        for (String entry : mapping.get().split(";")) {
            final String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }

            final int separator = trimmedEntry.indexOf('=');
            if (separator < 0) {
                LogManager.getLogger(JdbcParquetSource.class).warn(
                        "Ignoring invalid Parquet source mapping entry '{}' for {}.", trimmedEntry, mappingKey
                );
                continue;
            }

            final String sourceUri = trimmedEntry.substring(0, separator).trim();
            final String relationName = trimmedEntry.substring(separator + 1).trim();
            if (sourceUri.equals(inputUrl) && !relationName.isEmpty()) {
                return Optional.of(relationName);
            }
        }

        return Optional.empty();
    }

    private void executePrepareSql(Connection connection, Configuration configuration, String platformId) {
        final String prepareSqlKey = String.format("wayang.%s.parquetsource.prepare-sql", platformId);
        final Optional<String> optionalPrepareSql = configuration.getOptionalStringProperty(prepareSqlKey);
        if (optionalPrepareSql.isEmpty() || optionalPrepareSql.get().trim().isEmpty()) {
            return;
        }

        try (Statement statement = connection.createStatement()) {
            for (String sql : splitSqlStatements(resolvePlaceholders(optionalPrepareSql.get()))) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new WayangException(String.format(
                    "Could not execute Parquet source prepare SQL configured by '%s'.",
                    prepareSqlKey
            ), e);
        }
    }

    private static List<String> splitSqlStatements(String sql) {
        final List<String> statements = new ArrayList<>();
        final StringBuilder current = new StringBuilder(sql.length());
        boolean insideSingleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char currentChar = sql.charAt(i);
            if (currentChar == '\'') {
                current.append(currentChar);
                if (insideSingleQuote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    current.append(sql.charAt(++i));
                } else {
                    insideSingleQuote = !insideSingleQuote;
                }
            } else if (currentChar == ';' && !insideSingleQuote) {
                addStatement(statements, current);
            } else {
                current.append(currentChar);
            }
        }
        addStatement(statements, current);
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder statement) {
        final String sql = statement.toString().trim();
        if (!sql.isEmpty()) {
            statements.add(sql);
        }
        statement.setLength(0);
    }

    private static String resolvePlaceholders(String sql) {
        Matcher matcher = PROPERTY_PLACEHOLDER.matcher(sql);
        StringBuffer resolved = new StringBuffer();
        while (matcher.find()) {
            final String type = matcher.group(1);
            final String name = matcher.group(2);
            final String value = "env".equals(type) ? System.getenv(name) : System.getProperty(name);
            if (value == null) {
                throw new WayangException(String.format(
                        "Could not resolve Parquet source prepare SQL placeholder '${%s:%s}'.",
                        type,
                        name
                ));
            }
            matcher.appendReplacement(resolved, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(resolved);
        return resolved.toString();
    }

    private static boolean isAutoCreateEnabled(Configuration configuration, String platformId) {
        return configuration.getBooleanProperty(
                String.format("wayang.%s.parquetsource.auto-create", platformId),
                false
        );
    }

    private static String createGeneratedRelationName(Configuration configuration, String platformId, String inputUrl) {
        final String prefix = configuration.getStringProperty(
                String.format("wayang.%s.parquetsource.auto-create.relation-prefix", platformId),
                "wayang_parquet_"
        );
        return prefix + shortHash(inputUrl);
    }

    private static String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new WayangException("Could not create stable Parquet relation name.", e);
        }
    }

    private void createExternalRelation(Connection connection,
                                        Configuration configuration,
                                        String platformId,
                                        String relationName) {
        final String templateKey = String.format("wayang.%s.parquetsource.auto-create.template", platformId);
        final Optional<String> optionalTemplate = configuration.getOptionalStringProperty(templateKey);
        if (optionalTemplate.isEmpty()) {
            throw new WayangException(String.format(
                    "Parquet auto-create is enabled for platform '%s', but '%s' is not configured.",
                    platformId,
                    templateKey
            ));
        }

        final String template = optionalTemplate.get();
        final String ddl = template
                .replace("${relation}", relationName)
                .replace("${uri}", this.escapeSqlString(this.getInputUrl()))
                .replace("${columns}", this.createColumnDefinitions(templateKey, template));

        try (Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new WayangException(String.format(
                    "Could not create Parquet SQL relation '%s' for '%s'.",
                    relationName,
                    this.getInputUrl()
            ), e);
        }
    }

    private String escapeSqlString(String value) {
        return value.replace("'", "''");
    }

    private String createColumnDefinitions(String templateKey, String template) {
        if (!template.contains("${columns}")) {
            return "";
        }

        if (this.getSchema() == null || this.getSchema().getFields().isEmpty()) {
            throw new WayangException(String.format(
                    "Parquet source auto-create template '%s' uses ${columns}, but no Parquet schema is available. "
                            + "Create the source with ParquetSource.create(...) or configure a template that does not "
                            + "need explicit columns.",
                    templateKey
            ));
        }

        return this.getSchema().getFields().stream()
                .map(field -> field.getName() + " " + this.toSqlType(field))
                .reduce((left, right) -> left + ", " + right)
                .orElse("");
    }

    private String toSqlType(Type field) {
        if (!field.isPrimitive()) {
            return "VARCHAR";
        }

        final PrimitiveType primitiveType = field.asPrimitiveType();
        final LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                || logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
            return "VARCHAR";
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
                    (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
            return String.format("DECIMAL(%d,%d)", decimal.getPrecision(), decimal.getScale());
        }
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return "DATE";
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return "TIMESTAMP";
        }

        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                return "BOOLEAN";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case BINARY:
                return "VARCHAR";
            case FIXED_LEN_BYTE_ARRAY:
                return "VARBINARY";
            case INT96:
                return "TIMESTAMP";
            default:
                return "VARCHAR";
        }
    }
}
