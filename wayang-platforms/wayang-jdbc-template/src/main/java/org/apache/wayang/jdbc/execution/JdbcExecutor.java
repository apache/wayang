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

package org.apache.wayang.jdbc.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcGlobalReduceOperator;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.JdbcReduceByOperator;
import org.apache.wayang.jdbc.operators.JdbcSortOperator;
import org.apache.wayang.jdbc.operators.JdbcSourceOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    public static StringBuilder createSqlString(final JdbcExecutor jdbcExecutor,
                                                final JdbcSourceOperator sourceOp,
                                                final Collection<JdbcExecutionOperator> filterTasks,
                                                final JdbcProjectionOperator projectionTask,
                                                final JdbcGlobalReduceOperator globalReduceTask,
                                                final JdbcReduceByOperator reduceByTask,
                                                final JdbcSortOperator sortTask,
                                                final Collection<JdbcExecutionOperator> joinTasks) {
        return createSqlString(jdbcExecutor, sourceOp, filterTasks, projectionTask, globalReduceTask,
                reduceByTask, sortTask, joinTasks, null);
    }

    public static StringBuilder createSqlString(final JdbcExecutor jdbcExecutor,
                                                final JdbcSourceOperator sourceOp,
                                                final Collection<JdbcExecutionOperator> filterTasks,
                                                final JdbcProjectionOperator projectionTask,
                                                final JdbcGlobalReduceOperator globalReduceTask,
                                                final JdbcReduceByOperator reduceByTask,
                                                final JdbcSortOperator sortTask,
                                                final Collection<JdbcExecutionOperator> joinTasks,
                                                final Configuration configuration) {
        final String sourceName = sourceOp.createSqlClause(
                jdbcExecutor.connection,
                jdbcExecutor.functionCompiler,
                configuration
        );
        final Collection<String> conditions = filterTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler, configuration))
                .collect(Collectors.toList());
        final Collection<String> joins = joinTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler, configuration))
                .collect(Collectors.toList());

        final String selectClause;
        if (globalReduceTask != null) {
            selectClause = globalReduceTask.createSqlClause(
                    jdbcExecutor.connection,
                    jdbcExecutor.functionCompiler,
                    configuration
            );
        } else if (reduceByTask != null) {
            selectClause = reduceByTask.createSqlClause(
                    jdbcExecutor.connection,
                    jdbcExecutor.functionCompiler,
                    configuration
            );
        } else if (projectionTask != null) {
            selectClause = projectionTask.createSqlClause(
                    jdbcExecutor.connection,
                    jdbcExecutor.functionCompiler,
                    configuration
            );
        } else {
            selectClause = "*";
        }

        final StringBuilder sb = new StringBuilder(1000);
        sb.append("SELECT ").append(selectClause).append(" FROM ").append(sourceName);
        for (final String join : joins) {
            sb.append(" ").append(join);
        }
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(String.join(" AND ", conditions));
        }
        if (reduceByTask != null) {
            sb.append(" GROUP BY ").append(reduceByTask.getKeyDescriptor().getSqlImplementation().getField0());
        }
        if (sortTask != null) {
            sb.append(sortTask.createSqlClause(
                    jdbcExecutor.connection,
                    jdbcExecutor.functionCompiler,
                    configuration
            ));
        }

        // Intentionally no trailing ';'. A trailing semicolon is unnecessary for a
        // single-statement JDBC executeQuery and is rejected by strict SQL parsers
        // such as Trino and BigQuery. Postgres/SQLite/HSQLDB accept its absence.
        return sb;
    }

    /**
     * Creates a query channel and the SQL statement.
     */
    protected static Tuple2<String, SqlQueryChannel.Instance> createSqlQuery(final ExecutionStage stage,
                                                                             final OptimizationContext context,
                                                                             final JdbcExecutor jdbcExecutor) {
        final Collection<?> startTasks = stage.getStartTasks();
        JdbcExecutor.prepareSourceTasks(startTasks, jdbcExecutor, context.getConfiguration());
        final ExecutionTask startTask = JdbcExecutor.selectStartTask(startTasks, stage, context.getConfiguration());
        assert startTask.getOperator() instanceof JdbcSourceOperator
                : "Invalid JDBC stage: Start task has to be a JDBC source";

        final JdbcSourceOperator sourceOp = (JdbcSourceOperator) startTask.getOperator();
        SqlQueryChannel.Instance tipChannelInstance = JdbcExecutor.instantiateOutboundChannel(startTask, context,
                jdbcExecutor);
        final Collection<JdbcExecutionOperator> filterTasks = new ArrayList<>(4);
        JdbcProjectionOperator projectionTask = null;
        JdbcGlobalReduceOperator globalReduceTask = null;
        JdbcReduceByOperator reduceByTask = null;
        JdbcSortOperator sortTask = null;
        final Collection<JdbcExecutionOperator> joinTasks = new ArrayList<>();
        final Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size() <= 3;
        ExecutionTask nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            final ExecutionOperator operator = nextTask.getOperator();
            if (operator instanceof FilterOperator || operator instanceof SpatialFilterOperator) {
                filterTasks.add((JdbcExecutionOperator) operator);
            } else if (operator instanceof JdbcProjectionOperator) {
                assert projectionTask == null;
                projectionTask = (JdbcProjectionOperator) operator;
            } else if (operator instanceof final JdbcGlobalReduceOperator globalReduce) {
                assert globalReduceTask == null;
                globalReduceTask = globalReduce;
            } else if (operator instanceof final JdbcReduceByOperator reduceBy) {
                assert reduceByTask == null;
                reduceByTask = reduceBy;
            } else if (operator instanceof final JdbcSortOperator sort) {
                assert sortTask == null;
                sortTask = sort;
            } else if (operator instanceof JoinOperator || operator instanceof SpatialJoinOperator) {
                joinTasks.add((JdbcExecutionOperator) operator);
            } else {
                throw new WayangException(String.format("Unsupported JDBC execution task %s", nextTask));
            }

            tipChannelInstance = JdbcExecutor.instantiateOutboundChannel(nextTask, context, tipChannelInstance,
                    jdbcExecutor);
            nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        final StringBuilder query = createSqlString(jdbcExecutor, sourceOp, filterTasks, projectionTask,
                globalReduceTask, reduceByTask, sortTask, joinTasks, context.getConfiguration());
        return new Tuple2<>(query.toString(), tipChannelInstance);
    }

    /**
     * Selects the source that belongs on the left-hand side of a JDBC join.
     * Stage start tasks are not ordered, but {@link JdbcJoinOperator#createSqlClause}
     * assumes its first key descriptor's source is used in the {@code FROM} clause.
     */
    private static ExecutionTask selectStartTask(final Collection<?> startTasks,
                                                 final ExecutionStage stage,
                                                 final Configuration configuration) {
        if (startTasks.size() == 1) {
            return (ExecutionTask) startTasks.iterator().next();
        }

        for (ExecutionTask task : stage.getAllTasks()) {
            if (task.getOperator() instanceof JdbcJoinOperator) {
                final JdbcJoinOperator<?> joinOperator = (JdbcJoinOperator<?>) task.getOperator();
                final String leftSourceName = joinOperator.getKeyDescriptor0().getSqlImplementation().field0;
                for (Object startTaskObject : startTasks) {
                    final ExecutionTask startTask = (ExecutionTask) startTaskObject;
                    if (startTask.getOperator() instanceof JdbcSourceOperator) {
                        final JdbcSourceOperator sourceOperator = (JdbcSourceOperator) startTask.getOperator();
                        if (sourceOperator.getSourceName().equals(leftSourceName)
                                || sourceOperator.getSourceName(configuration).equals(leftSourceName)) {
                            return startTask;
                        }
                    }
                }
            }
        }

        throw new WayangException("Could not determine the left source for JDBC stage.");
    }

    private static void prepareSourceTasks(final Collection<?> startTasks,
                                           final JdbcExecutor jdbcExecutor,
                                           final Configuration configuration) {
        for (Object startTaskObject : startTasks) {
            final ExecutionTask startTask = (ExecutionTask) startTaskObject;
            if (startTask.getOperator() instanceof JdbcSourceOperator) {
                ((JdbcSourceOperator) startTask.getOperator()).prepareSource(
                        jdbcExecutor.connection,
                        jdbcExecutor.functionCompiler,
                        configuration
                );
            }
        }
    }

    /**
     * Handles execution stages that end with a {@link JdbcTableSinkOperator}.
     */
    private static long executeSinkStage(final ExecutionStage stage, final OptimizationContext optimizationContext,
            final JdbcExecutor jdbcExecutor) {
        final Collection<?> startTasks = stage.getStartTasks();
        final Collection<?> termTasks = stage.getTerminalTasks();
        JdbcExecutor.prepareSourceTasks(startTasks, jdbcExecutor, optimizationContext.getConfiguration());

        final ExecutionTask startTask = JdbcExecutor.selectStartTask(
                startTasks,
                stage,
                optimizationContext.getConfiguration()
        );
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        assert startTask.getOperator() instanceof JdbcSourceOperator
                : "Invalid JDBC stage: Start task has to be a JDBC source";
        assert termTask.getOperator() instanceof JdbcTableSinkOperator
                : "Invalid JDBC stage: Terminal task has to be a JdbcTableSinkOperator";

        final JdbcSourceOperator sourceOp = (JdbcSourceOperator) startTask.getOperator();
        final JdbcTableSinkOperator sinkOp = (JdbcTableSinkOperator) termTask.getOperator();
        final Collection<JdbcExecutionOperator> filterTasks = new ArrayList<>(4);
        JdbcProjectionOperator projectionTask = null;
        JdbcGlobalReduceOperator globalReduceTask = null;
        JdbcReduceByOperator reduceByTask = null;
        JdbcSortOperator sortTask = null;
        final Collection<JdbcExecutionOperator> joinTasks = new ArrayList<>();

        ExecutionTask nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null && !(nextTask.getOperator() instanceof JdbcTableSinkOperator)) {
            if (nextTask.getOperator() instanceof final JdbcFilterOperator filterOperator) {
                filterTasks.add(filterOperator);
            } else if (nextTask.getOperator() instanceof final JdbcProjectionOperator projectionOperator) {
                assert projectionTask == null;
                projectionTask = projectionOperator;
            } else if (nextTask.getOperator() instanceof final JdbcGlobalReduceOperator globalReduceOperator) {
                assert globalReduceTask == null;
                globalReduceTask = globalReduceOperator;
            } else if (nextTask.getOperator() instanceof final JdbcReduceByOperator reduceByOperator) {
                assert reduceByTask == null;
                reduceByTask = reduceByOperator;
            } else if (nextTask.getOperator() instanceof final JdbcSortOperator sortOperator) {
                assert sortTask == null;
                sortTask = sortOperator;
            } else if (nextTask.getOperator() instanceof final JdbcJoinOperator joinOperator) {
                joinTasks.add(joinOperator);
            } else {
                throw new WayangException(String.format("Unsupported JDBC execution task %s", nextTask));
            }
            nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        final String selectSql = createSqlString(jdbcExecutor, sourceOp, filterTasks, projectionTask,
                globalReduceTask, reduceByTask, sortTask, joinTasks, optimizationContext.getConfiguration()).toString();
        final String sinkClause = sinkOp.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);

        try (Statement stmt = jdbcExecutor.connection.createStatement()) {
            if ("overwrite".equals(sinkOp.getMode())) {
                stmt.execute("DROP TABLE IF EXISTS " + sinkOp.getTableName());
            }
            final String fullSql = sinkClause + " " + selectSql + sinkOp.createSqlSuffix();
            final long startTime = System.currentTimeMillis();
            stmt.execute(fullSql);
            final long executionDuration = System.currentTimeMillis() - startTime;
            jdbcExecutor.logger.info("Executed SQL sink: {}", fullSql);
            System.out.println("Executed sql sink: " + fullSql);
            return executionDuration;
        } catch (final SQLException e) {
            throw new WayangException("Failed to execute SQL sink on table: " + sinkOp.getTableName(), e);
        }
    }

    /**
     * Creates lineage nodes for the JDBC operators that were executed as one SQL
     * statement. Operators without an optimization context or load estimator are
     * skipped, so JDBC platforms without cost specifications can still execute.
     */
    private Collection<ExecutionLineageNode> createExecutionLineageNodes(
            final ExecutionStage stage,
            final OptimizationContext optimizationContext) {
        final Collection<ExecutionLineageNode> executionLineageNodes = new ArrayList<>();
        for (ExecutionTask task : stage.getAllTasks()) {
            final OptimizationContext.OperatorContext operatorContext =
                    optimizationContext.getOperatorContext(task.getOperator());
            if (operatorContext == null) {
                this.logger.warn("Cannot profile {} because its optimization context is missing.", task);
                continue;
            }
            if (operatorContext.getLoadProfileEstimator() == null) {
                this.logger.warn("Cannot profile {} because its load profile estimator is missing.", task);
                continue;
            }
            executionLineageNodes.add(
                    new ExecutionLineageNode(operatorContext).addAtomicExecutionFromOperatorContext());
        }
        return executionLineageNodes;
    }

    /**
     * Retrieves the follow-up {@link ExecutionTask} of the given {@code task}
     * unless it is not comprising a {@link JdbcExecutionOperator} and/or not in the
     * given {@link ExecutionStage}.
     *
     * @param task  whose follow-up {@link ExecutionTask} is requested; should have
     *              a single follower
     * @param stage in which the follow-up {@link ExecutionTask} should be
     * @return the said follow-up {@link ExecutionTask} or {@code null} if none
     */
    private static ExecutionTask findJdbcExecutionOperatorTaskInStage(final ExecutionTask task,
                                                                      final ExecutionStage stage) {
        assert task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);
        if (outputChannel.getConsumers().size() != 1) {
            return null;
        }

        final ExecutionTask consumer = outputChannel.getConsumers().iterator().next();
        return consumer.getStage() == stage && consumer.getOperator() instanceof JdbcExecutionOperator
                ? consumer
                : null;
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                whose outbound {@link SqlQueryChannel} should be
     *                            instantiated
     * @param optimizationContext provides information about the
     *                            {@link ExecutionTask}
     * @return the {@link SqlQueryChannel.Instance}
     */
    private static SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
                                                                       final OptimizationContext optimizationContext,
                                                                       final JdbcExecutor jdbcExecutor) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        final SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext
                .getOperatorContext(task.getOperator());
        return outputChannel.createInstance(jdbcExecutor, operatorContext, 0);
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                       whose outbound {@link SqlQueryChannel}
     *                                   should be instantiated
     * @param optimizationContext        provides information about the
     *                                   {@link ExecutionTask}
     * @param predecessorChannelInstance preceeding {@link SqlQueryChannel.Instance}
     *                                   to keep track of lineage
     * @return the {@link SqlQueryChannel.Instance}
     */
    private static SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
                                                                       final OptimizationContext optimizationContext,
                                                                       final SqlQueryChannel.Instance predecessorChannelInstance,
                                                                       final JdbcExecutor jdbcExecutor) {
        final SqlQueryChannel.Instance newInstance = JdbcExecutor.instantiateOutboundChannel(task, optimizationContext,
                jdbcExecutor);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext,
                        final ExecutionState executionState) {
        final Collection<?> termTasks = stage.getTerminalTasks();
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];

        if (termTask.getOperator() instanceof JdbcTableSinkOperator) {
            final long executionDuration = JdbcExecutor.executeSinkStage(stage, optimizationContext, this);
            if (this.isProfilingEnabled()) {
                final PartialExecution partialExecution = this.createPartialExecution(
                        this.createExecutionLineageNodes(stage, optimizationContext),
                        executionDuration
                );
                if (partialExecution != null) {
                    executionState.add(partialExecution);
                }
            }
        } else {
            final Tuple2<String, SqlQueryChannel.Instance> pair = JdbcExecutor.createSqlQuery(stage,
                    optimizationContext, this);
            final String query = pair.field0;
            final SqlQueryChannel.Instance queryChannel = pair.field1;
            queryChannel.setSqlQuery(query);
            executionState.register(queryChannel);
        }
    }

    private boolean isProfilingEnabled() {
        return this.getConfiguration().getBooleanProperty("wayang.core.log.enabled", false);
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (final SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }
}
