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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvSelectionOperationsDemo {

    private static final Path DEFAULT_DATA_DIRECTORY = Paths.get("wayang-jdbc", "demo", "data");

    private static final String DEFAULT_SCHEMA = "fs";

    private static final String DEFAULT_JDBC_URL = "jdbc:wayang://127.0.0.1:9999/demo";

    private static final int SAMPLE_ROW_LIMIT = 5;

    public static void main(final String[] args) throws Exception {
        final Path dataDirectory = args.length > 0 ? Paths.get(args[0]) : DEFAULT_DATA_DIRECTORY;
        final String schema = args.length > 1 ? args[1] : DEFAULT_SCHEMA;
        final String jdbcUrl = args.length > 2 ? args[2] : DEFAULT_JDBC_URL;

        final List<Path> csvFiles = listCsvFiles(dataDirectory);
        if (csvFiles.isEmpty()) {
            System.out.println("No CSV files found in " + dataDirectory.toAbsolutePath().normalize());
            return;
        }

        printAvailableCsvFiles(csvFiles, schema);

        final Path selectedFile = selectCsvFile(csvFiles);
        final String tableName = tableName(selectedFile);
        final String qualifiedTableName = schema + "." + tableName;

        System.out.println();
        System.out.println("Selected file: " + selectedFile.getFileName());
        System.out.println("SQL table name: " + qualifiedTableName);
        System.out.println("JDBC URL: " + jdbcUrl);

        Class.forName("org.apache.wayang.jdbc.driver.WayangDriver");

        final Properties properties = new Properties();
        properties.setProperty("user", "demo");
        properties.setProperty("connectTimeout", "5000");

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement statement = connection.createStatement()) {
            System.out.println();
            System.out.println("Connected to " + jdbcUrl);

            runCountQuery(statement, qualifiedTableName);
            runPreviewQuery(statement, qualifiedTableName);
        }
    }

    private static List<Path> listCsvFiles(final Path dataDirectory) throws Exception {
        if (!Files.isDirectory(dataDirectory)) {
            throw new IllegalArgumentException(
                    "Data directory does not exist: " + dataDirectory.toAbsolutePath().normalize()
            );
        }

        try (Stream<Path> files = Files.list(dataDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(CsvSelectionOperationsDemo::isCsvFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    private static void printAvailableCsvFiles(final List<Path> csvFiles, final String schema) {
        System.out.println("Available CSV files:");
        for (int index = 0; index < csvFiles.size(); index++) {
            final Path csvFile = csvFiles.get(index);
            System.out.printf(
                    Locale.ROOT,
                    "%d. %-40s SQL table: %s.%s%n",
                    index + 1,
                    csvFile.getFileName(),
                    schema,
                    tableName(csvFile)
            );
        }
    }

    private static Path selectCsvFile(final List<Path> csvFiles) {
        System.out.println();
        System.out.print("Select CSV by number or name, for example heart_disease_risk: ");

        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                final String input = scanner.nextLine().trim();
                final Optional<Path> selected = resolveSelection(input, csvFiles);
                if (selected.isPresent()) {
                    return selected.get();
                }

                System.out.print("Invalid selection. Try number or CSV/table name: ");
            }
        }

        throw new IllegalArgumentException("No CSV file was selected.");
    }

    private static Optional<Path> resolveSelection(final String input, final List<Path> csvFiles) {
        if (input.isBlank()) {
            return Optional.empty();
        }

        try {
            final int selectedIndex = Integer.parseInt(input);
            if (selectedIndex >= 1 && selectedIndex <= csvFiles.size()) {
                return Optional.of(csvFiles.get(selectedIndex - 1));
            }
        } catch (NumberFormatException ignored) {
            // Continue with name-based matching.
        }

        final String normalizedInput = normalize(input);
        return csvFiles.stream()
                .filter(path -> {
                    final String fileName = normalize(path.getFileName().toString());
                    final String tableName = normalize(tableName(path));
                    return fileName.equals(normalizedInput)
                            || tableName.equals(normalizedInput)
                            || fileName.contains(normalizedInput)
                            || tableName.contains(normalizedInput);
                })
                .findFirst();
    }

    private static void runCountQuery(final Statement statement, final String qualifiedTableName) throws Exception {
        final String countSql = "SELECT COUNT(*) AS total_rows FROM " + qualifiedTableName;
        System.out.println();
        System.out.println("Query: " + countSql);

        try (ResultSet resultSet = statement.executeQuery(countSql)) {
            if (resultSet.next()) {
                System.out.println("Total rows: " + resultSet.getObject(1));
            }
        }
    }

    private static void runPreviewQuery(final Statement statement, final String qualifiedTableName) throws Exception {
        final String previewSql = "SELECT * FROM " + qualifiedTableName + " LIMIT " + SAMPLE_ROW_LIMIT;
        System.out.println();
        System.out.println("Query: " + previewSql);

        try (ResultSet resultSet = statement.executeQuery(previewSql)) {
            printResultSet(resultSet);
        }
    }

    private static void printResultSet(final ResultSet resultSet) throws Exception {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        int rowCount = 0;

        while (resultSet.next()) {
            rowCount++;
            System.out.println("Row " + rowCount + ":");
            for (int column = 1; column <= columnCount; column++) {
                System.out.println("  " + metaData.getColumnLabel(column) + " = " + resultSet.getObject(column));
            }
        }

        if (rowCount == 0) {
            System.out.println("No rows returned.");
        }
    }

    private static boolean isCsvFile(final Path path) {
        return path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    private static String tableName(final Path csvFile) {
        final String fileName = csvFile.getFileName().toString();
        return fileName.substring(0, fileName.length() - ".csv".length());
    }

    private static String normalize(final String value) {
        return value.toLowerCase(Locale.ROOT).replace(".csv", "").trim();
    }
}
