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

package org.apache.wayang.genericjdbc.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.genericjdbc.operators.GenericJdbcExecutionOperator;
import org.apache.wayang.genericjdbc.operators.GenericJdbcFilterOperator;
import org.apache.wayang.genericjdbc.operators.GenericJdbcProjectionOperator;
import org.apache.wayang.genericjdbc.operators.GenericJdbcTableSource;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link GenericJdbcPlatform}.
 */
public class GenericJdbcExecutor extends ExecutorTemplate {

    private final GenericJdbcPlatform platform;

    private final Connection connection;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public GenericJdbcExecutor(GenericJdbcPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration(), "genericjdbc")
                .createJdbcConnection();
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        // TODO: Load ChannelInstances from executionState? (as of now there is no input into PostgreSQL).
        Collection<?> startTasks = stage.getStartTasks();
        Collection<?> termTasks = stage.getTerminalTasks();

        // Verify that we can handle this instance.
        assert startTasks.size() == 1 : "Invalid jdbc stage: multiple sources are not currently supported";
        ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        assert startTask.getOperator() instanceof GenericJdbcTableSource : "Invalid JDBC stage: Start task has to be a TableSource";

        // Extract the different types of ExecutionOperators from the stage.
        GenericJdbcTableSource tableOp = (GenericJdbcTableSource) startTask.getOperator();
        SqlQueryChannel.Instance tipChannelInstance = this.instantiateOutboundChannel(startTask, optimizationContext);
        Collection<ExecutionTask> filterTasks = new ArrayList<>(4);
        ExecutionTask projectionTask = null;
        Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size() <= 3;
        ExecutionTask nextTask = this.findGenericJdbcExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            // Evaluate the nextTask.
            if (nextTask.getOperator() instanceof GenericJdbcFilterOperator) {
                filterTasks.add(nextTask);
            } else if (nextTask.getOperator() instanceof GenericJdbcProjectionOperator) {
                assert projectionTask == null; //Allow one projection operator per stage for now.
                projectionTask = nextTask;

            } else {
                throw new WayangException(String.format("Unsupported JDBC execution task %s", nextTask.toString()));
            }

            // Move the tipChannelInstance.
            tipChannelInstance = this.instantiateOutboundChannel(nextTask, optimizationContext, tipChannelInstance);

            // Go to the next nextTask.
            nextTask = this.findGenericJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        // Create the SQL query.
        String tableName = this.getSqlClause(tableOp);
        String jdbcName = tableOp.jdbcName;
        Collection<String> conditions = filterTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        String projection = projectionTask == null ? "*" : this.getSqlClause(projectionTask.getOperator());
        String query = this.createSqlQuery(tableName, conditions, projection);
        tipChannelInstance.setSqlQuery(query);
        tipChannelInstance.setJdbcName(jdbcName);

        // Return the tipChannelInstance.
        executionState.register(tipChannelInstance);
    }

    /**
     * Retrieves the follow-up {@link ExecutionTask} of the given {@code task} unless it is not comprising a
     * {@link GenericJdbcExecutionOperator} and/or not in the given {@link ExecutionStage}.
     *
     * @param task  whose follow-up {@link ExecutionTask} is requested; should have a single follower
     * @param stage in which the follow-up {@link ExecutionTask} should be
     * @return the said follow-up {@link ExecutionTask} or {@code null} if none
     */
    private ExecutionTask findGenericJdbcExecutionOperatorTaskInStage(ExecutionTask task, ExecutionStage stage) {
        assert task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);

        if (outputChannel.getConsumers().isEmpty()) {
            return null;
        }

        final ExecutionTask consumer = WayangCollections.getSingle(outputChannel.getConsumers());

        return consumer.getStage() == stage && consumer.getOperator() instanceof GenericJdbcExecutionOperator ?   
                consumer :
                null;
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an {@link ExecutionTask}.
     *
     * @param task                whose outbound {@link SqlQueryChannel} should be instantiated
     * @param optimizationContext provides information about the {@link ExecutionTask}
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                       OptimizationContext optimizationContext) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(task.getOperator());
        return outputChannel.createInstance(this, operatorContext, 0);
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an {@link ExecutionTask}.
     *
     * @param task                       whose outbound {@link SqlQueryChannel} should be instantiated
     * @param optimizationContext        provides information about the {@link ExecutionTask}
     * @param predecessorChannelInstance preceeding {@link SqlQueryChannel.Instance} to keep track of lineage
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                       OptimizationContext optimizationContext,
                                                                       SqlQueryChannel.Instance predecessorChannelInstance) {
        final SqlQueryChannel.Instance newInstance = this.instantiateOutboundChannel(task, optimizationContext);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }

    /**
     * Creates a SQL query.
     *
     * @param tableName  the table to be queried
     * @param conditions conditions for the {@code WHERE} clause
     * @param projection projection for the {@code SELECT} clause
     * @return the SQL query
     */
    protected String createSqlQuery(String tableName, Collection<String> conditions, String projection) {
        StringBuilder sb = new StringBuilder(1000);
        sb.append("SELECT ").append(projection).append(" FROM ").append(tableName);
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }
        sb.append(';');
        return sb.toString();
    }

    /**
     * Creates a SQL clause that corresponds to the given {@link Operator}.
     *
     * @param operator for that the SQL clause should be generated
     * @return the SQL clause
     */
    private String getSqlClause(Operator operator) {
        return ((GenericJdbcExecutionOperator) operator).createSqlClause(this.connection, this.functionCompiler);
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }


    private void saveResult(FileChannel.Instance outputFileChannelInstance, ResultSet rs) throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                //System.out.println(rs.getInt(1) + " " + rs.getString(2));
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
