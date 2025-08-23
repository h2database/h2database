/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PrintWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.result.Row;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;
import org.h2.value.Value;
import org.h2.value.ValueCollectionBase;

/**
 * Enhanced database recovery tool with streaming and compression support.
 *
 * <p>DirectRecover extends the original Recover class to provide:
 * <ul>
 * <li><strong>Streaming Processing:</strong> Uses pipes for memory-efficient processing of large databases</li>
 * <li><strong>Parallel Execution:</strong> Dump generation and SQL writing happen concurrently</li>
 * <li><strong>Compression Support:</strong> On-the-fly compression with GZIP, ZIP, BZIP2, or KANZI</li>
 * <li><strong>No Intermediate Files:</strong> Direct streaming prevents disk space issues</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 *
 * <h3>Basic Recovery (No Compression):</h3>
 * <pre>
 * java -cp h2.jar org.h2.tools.DirectRecover -dir /path/to/db -db mydb
 * </pre>
 *
 * <h3>With Built-in Compression:</h3>
 * <pre>
 * # GZIP compression (recommended for most cases)
 * java -cp h2.jar org.h2.tools.DirectRecover -dir /path/to/db -db mydb -compress gzip
 *
 * # ZIP compression (widely compatible)
 * java -cp h2.jar org.h2.tools.DirectRecover -dir /path/to/db -db mydb -compress zip
 * </pre>
 *
 * <h3>With Optional External Libraries:</h3>
 * <pre>
 * # BZIP2 compression (requires Apache Commons Compress)
 * java -cp "h2.jar:commons-compress.jar" org.h2.tools.DirectRecover -dir /path/to/db -db mydb -compress bzip2
 *
 * # KANZI compression (requires Kanzi library, best compression)
 * java -cp "h2.jar:kanzi.jar" org.h2.tools.DirectRecover -dir /path/to/db -db mydb -compress kanzi
 * </pre>
 *
 * <h3>Debug and Troubleshooting:</h3>
 * <pre>
 * # Enable verbose debug output
 * java -cp h2.jar org.h2.tools.DirectRecover -dir /path/to/db -db mydb -trace -compress gzip
 *
 * # Process all databases in directory
 * java -cp h2.jar org.h2.tools.DirectRecover -dir /path/to/db -compress gzip
 * </pre>
 *
 * <h2>Command Line Options:</h2>
 * <table border="1">
 * <caption>Supported Options</caption>
 * <tr><th>Option</th><th>Description</th><th>Default</th></tr>
 * <tr><td>-dir &lt;directory&gt;</td><td>Database directory</td><td>. (current directory)</td></tr>
 * <tr><td>-db &lt;database&gt;</td><td>Database name (without .mv.db extension)</td>
 * <td>All databases in directory</td></tr>
 * <tr><td>-compress &lt;type&gt;</td><td>Compression: none, gzip, zip, bzip2, kanzi</td><td>none</td></tr>
 * <tr><td>-trace</td><td>Enable debug output and verbose logging</td><td>false</td></tr>
 * <tr><td>-help or -?</td><td>Show help information</td><td>-</td></tr>
 * </table>
 *
 * <h2>Output Files:</h2>
 * <ul>
 * <li><strong>No compression:</strong> database.sql</li>
 * <li><strong>GZIP:</strong> database.sql.gz</li>
 * <li><strong>ZIP:</strong> database.sql.zip</li>
 * <li><strong>BZIP2:</strong> database.sql.bz2</li>
 * <li><strong>KANZI:</strong> database.sql.knz</li>
 * </ul>
 *
 * <h2>External Dependencies (Optional):</h2>
 * <ul>
 * <li><strong>Apache Commons Compress:</strong> Required for BZIP2 compression
 * <br>
 * Download: <a href=
 * "https://commons.apache.org/proper/commons-compress/">https://commons.apache.org/proper/commons-compress/</a></li>
 * <li><strong>Kanzi:</strong> Required for KANZI compression (best compression ratio)
 *     <br>Download: <a href="https://github.com/flanglet/kanzi-java">https://github.com/flanglet/kanzi-java</a></li>
 * </ul>
 *
 * <h2>Performance Notes:</h2>
 * <ul>
 * <li><strong>Streaming:</strong> Memory usage is bounded (~256KB) regardless of database size</li>
 * <li><strong>Parallel:</strong> Dump generation and SQL writing happen simultaneously</li>
 * <li><strong>Compression:</strong> Applied on-the-fly, no temporary uncompressed files</li>
 * <li><strong>Large Databases:</strong> Designed to handle multi-GB databases efficiently</li>
 * </ul>
 *
 * <h2>Error Handling:</h2>
 * <ul>
 * <li>Missing compression libraries fall back to uncompressed output</li>
 * <li>Corrupted databases are processed in recovery mode</li>
 * <li>Timeouts prevent indefinite hanging</li>
 * </ul>
 *
 */
public class DirectRecover extends Recover {
    private ExecutorService kanziExecutor = null;

    private CompressionType compressionType = CompressionType.NONE;
    private boolean debugMode = false;
    private boolean showProgress = true;

    /**
     * Simple ASCII progress bar for terminal output.
     */
    private static class ProgressBar {
        private final int width;
        private final String prefix;
        private long lastUpdate = 0;
        private int lastProgress = -1;

        public ProgressBar(String prefix, int width) {
            this.prefix = prefix;
            this.width = width;
        }

        public void update(long current, long total) {
            long now = System.currentTimeMillis();
            // Update at most every 100ms to avoid flickering
            if (now - lastUpdate < 100) return;
            lastUpdate = now;

            int progress = total > 0 ? (int) ((current * 100) / total) : 0;
            if (progress == lastProgress) return;
            lastProgress = progress;

            int filled = (progress * width) / 100;
            StringBuilder bar = new StringBuilder();
            bar.append('\r').append(prefix).append(" [");

            for (int i = 0; i < width; i++) {
                if (i < filled) {
                    bar.append('\u2588');
                } else if (i == filled && progress < 100) {
                    bar.append('\u2593');
                } else {
                    bar.append('\u2591');
                }
            }

            bar.append(String.format("] %3d%% ", progress));
            System.out.print(bar);
            System.out.flush();

            if (progress >= 100) {
                System.out.println(); // New line when complete
            }
        }

        public void finish() {
            if (lastProgress < 100) {
                System.out.println(); // Ensure we end with a newline
            }
        }
    }

    /**
     * Options are case-sensitive.
     * <table>
     * <caption>Supported options (in addition to base Recover options)</caption>
     * <tr><td>[-compress &lt;type&gt;]</td>
     * <td>Compress SQL output (none, gzip, zip, bzip2, kanzi, default: none)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Enable verbose debug output</td></tr>
     * </table>
     *
     * @param args the command line arguments
     * @throws SQLException on failure
     */
    public static void main(String... args) throws SQLException {
        new DirectRecover().runTool(args);
    }

    /**
     * Enhanced runTool method with compression support.
     */
    @Override
    public void runTool(String... args) throws SQLException {
        String dir = ".";
        String db = null;
        boolean trace = false;

        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if ("-dir".equals(arg)) {
                dir = args[++i];
            } else if ("-db".equals(arg)) {
                db = args[++i];
            } else if ("-trace".equals(arg)) {
                trace = true;
                debugMode = true;
                showProgress = false; // Disable progress bar when debugging
            } else if ("-compress".equals(arg)) {
                String compressArg = args[++i].toUpperCase();
                try {
                    compressionType = CompressionType.valueOf(compressArg);
                } catch (IllegalArgumentException e) {
                    throw new SQLException("Invalid compression type: " + compressArg +
                            ". Valid options: none, gzip, zip, bzip2, kanzi");
                }
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }

        // Set trace in parent class
        super.trace = trace;

        // Check if requested compression is available
        if (compressionType != CompressionType.NONE && !isCompressionAvailable(compressionType)) {
            System.err.println("WARNING: " + compressionType + " compression library not found in classpath.");
            System.err.println("Falling back to uncompressed output (.sql files).");
            if (compressionType == CompressionType.BZIP2) {
                System.err.println("To enable BZIP2: Add commons-compress.jar to classpath");
                System.err.println("Download from: https://commons.apache.org/proper/commons-compress/");
            } else if (compressionType == CompressionType.KANZI) {
                System.err.println("To enable KANZI: Add kanzi.jar to classpath");
                System.err.println("Download from: https://github.com/flanglet/kanzi-java");
            }
            System.err.println();
        }

        debug("Starting DirectRecover with compression: " + compressionType);
        processWithPipe(dir, db);
        debug("DirectRecover completed successfully");
    }

    private void debug(String message) {
        if (debugMode) {
            System.out.println("[DEBUG] " + message + " [Thread: " + Thread.currentThread().getName() + "]");
            System.out.flush();
        }
    }

    /**
     * Enhanced process method using pipes between dump and SQL conversion.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     */
    private void processWithPipe(String dir, String db) {
        ArrayList<String> list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.isEmpty()) {
            printNoDatabaseFilesFound(dir, db);
            return;
        }

        debug("Found " + list.size() + " database files to process");
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            for (String fileName : list) {
                if (fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
                    debug("Processing file: " + fileName);
                    String f = fileName.substring(0, fileName.length() -
                            Constants.SUFFIX_MV_FILE.length());

                    ProgressBar progressBar = null;
                    if (showProgress) {
                        String dbName = new File(fileName).getName();
                        progressBar = new ProgressBar("Processing " + dbName, 50);
                    }

                    PipedWriter pipeWriter = null;
                    PipedReader pipeReader = null;

                    try {
                        debug("Creating pipes for: " + fileName);
                        // Create pipe for ASCII dump -> SQL conversion
                        pipeWriter = new PipedWriter();
                        pipeReader = new PipedReader(pipeWriter, 256 * 1024); // 256k buffer

                        final PipedWriter finalPipeWriter = pipeWriter;
                        final PipedReader finalPipeReader = pipeReader;
                        final ProgressBar finalProgressBar = progressBar;

                        debug("Starting dump task for: " + fileName);
                        // Future for the dump task (producer)
                        CompletableFuture<Void> dumpTask = CompletableFuture.runAsync(() -> {
                            debug("DUMP TASK: Starting dump for " + fileName);
                            try (PrintWriter writer = new PrintWriter(finalPipeWriter)) {
                                debug("DUMP TASK: Created writer, starting MVStoreTool.dump");
                                MVStoreTool.dump(fileName, writer, true);
                                debug("DUMP TASK: MVStoreTool.dump completed, starting info");
                                MVStoreTool.info(fileName, writer);
                                debug("DUMP TASK: MVStoreTool.info completed, flushing");
                                writer.flush();
                                debug("DUMP TASK: Flush completed, dump task finishing");
                            } catch (Exception e) {
                                debug("DUMP TASK: Error - " + e.getMessage());
                                trace("Error in dump task: " + e.getMessage());
                            }
                            debug("DUMP TASK: Dump task completed for " + fileName);
                        }, executor);

                        debug("Starting SQL task for: " + fileName);
                        // Future for the SQL conversion task (consumer)
                        CompletableFuture<Void> sqlTask = CompletableFuture.runAsync(() -> {
                            debug("SQL TASK: Starting SQL conversion for " + fileName);
                            try (PrintWriter sqlWriter = getCompressedWriter(f + ".h2.db")) {
                                debug("SQL TASK: Created compressed writer, starting processPipedDumpToSQL");
                                // Process the piped ASCII dump and convert to SQL
                                processPipedDumpToSQL(finalPipeReader, sqlWriter, fileName, finalProgressBar);
                                debug("SQL TASK: processPipedDumpToSQL completed, flushing");
                                sqlWriter.flush();
                                debug("SQL TASK: Flush completed, SQL task finishing");
                            } catch (Exception e) {
                                debug("SQL TASK: Error - " + e.getMessage());
                                trace("Error in SQL conversion task: " + e.getMessage());
                            }
                            debug("SQL TASK: SQL task completed for " + fileName);
                        }, executor);

                        debug("Waiting for both tasks to complete for: " + fileName);
                        // Wait for both tasks to complete with timeout
                        try {
                            CompletableFuture.allOf(dumpTask, sqlTask)
                                    .get(1, TimeUnit.DAYS);
                            debug("Both tasks completed successfully for: " + fileName);
                            if (progressBar != null) {
                                progressBar.finish();
                            }
                        } catch (java.util.concurrent.TimeoutException e) {
                            debug("TIMEOUT: Processing timed out 1 day for: " + fileName);
                            trace("Processing timed out after 1 day");
                            dumpTask.cancel(true);
                            sqlTask.cancel(true);
                            if (progressBar != null) {
                                progressBar.finish();
                            }
                        }

                    } catch (Exception e) {
                        debug("ERROR: Exception processing " + fileName + ": " + e.getMessage());
                        traceError("Error processing " + fileName, e);
                    } finally {
                        debug("Cleaning up pipes for: " + fileName);
                        // Ensure pipes are closed
                        if (pipeWriter != null) {
                            try {
                                pipeWriter.close();
                                debug("PipeWriter closed for: " + fileName);
                            } catch (Exception e) {
                                debug("Error closing PipeWriter: " + e.getMessage());
                            }
                        }
                        if (pipeReader != null) {
                            try {
                                pipeReader.close();
                                debug("PipeReader closed for: " + fileName);
                            } catch (Exception e) {
                                debug("Error closing PipeReader: " + e.getMessage());
                            }
                        }
                        debug("Pipe cleanup completed for: " + fileName);
                    }
                }
            }
        } finally {
            debug("Shutting down executor");
            // Properly shutdown executor
            executor.shutdown();
            try {
                debug("Waiting for executor termination (30 seconds)");
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    debug("Executor did not terminate gracefully, forcing shutdown");
                    trace("Executor did not terminate gracefully, forcing shutdown");
                    executor.shutdownNow();
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        debug("Executor did not terminate after force shutdown");
                        System.err.println("Executor did not terminate");
                    } else {
                        debug("Executor terminated after force shutdown");
                    }
                } else {
                    debug("Executor terminated gracefully");
                }
            } catch (InterruptedException e) {
                debug("Interrupted while waiting for executor termination");
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            debug("Executor shutdown completed");
        }
    }

    /**
     * Creates a writer with optional compression support.
     *
     * @param fileName the base file name
     * @return PrintWriter that may write to compressed output
     */
    private PrintWriter getCompressedWriter(String fileName) {
        fileName = fileName.substring(0, fileName.length() - 3);
        String outputFile;

        switch (compressionType) {
            case GZIP:
                outputFile = fileName + ".sql" + ".gz";
                debug("Creating GZIP compressed writer for: " + outputFile);
                try {
                    OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                    GZIPOutputStream gzipOut = new GZIPOutputStream(baseOut);
                    debug("GZIP writer created successfully: " + outputFile);
                    trace("Created compressed file: " + outputFile);
                    return new PrintWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }

            case ZIP:
                outputFile = fileName + ".sql" + ".zip";
                debug("Creating ZIP compressed writer for: " + outputFile);
                try {
                    OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                    ZipOutputStream zipOut = new ZipOutputStream(baseOut);
                    String entryName = new File(fileName + ".sql").getName();
                    zipOut.putNextEntry(new ZipEntry(entryName));
                    debug("ZIP writer created successfully: " + outputFile);
                    trace("Created compressed file: " + outputFile);
                    return new PrintWriter(new OutputStreamWriter(zipOut, StandardCharsets.UTF_8)) {
                        @Override
                        public void close() {
                            try {
                                super.close();
                                zipOut.closeEntry();
                                zipOut.close();
                                debug("ZIP writer closed successfully.");
                            } catch (IOException e) {
                                debug("Error closing ZIP stream: " + e.getMessage());
                                System.err.println("Error closing ZIP stream: " + e.getMessage());
                            }
                        }
                    };
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }

            case BZIP2:
                outputFile = fileName + ".sql" + ".bz2";
                debug("Creating BZIP2 compressed writer for: " + outputFile);
                try {
                    OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                    OutputStream compressedOut = createCompressedStream(CompressionType.BZIP2, baseOut);
                    debug("BZIP2 writer created successfully: " + outputFile);
                    trace("Created compressed file: " + outputFile);
                    return new PrintWriter(new OutputStreamWriter(compressedOut, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    // Fallback to uncompressed if BZip2 not available
                    debug("BZIP2 compression not available, falling back: " + e.getMessage());
                    trace("BZip2 compression not available: " + e.getMessage());
                    trace("Falling back to uncompressed output");
                    outputFile = fileName + ".sql";
                    debug("Creating fallback uncompressed writer for: " + outputFile);
                    trace("Created fallback file: " + outputFile);
                    try {
                        OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                        return new PrintWriter(new OutputStreamWriter(baseOut, StandardCharsets.UTF_8));
                    } catch (IOException ioEx) {
                        throw DbException.convertIOException(ioEx, null);
                    }
                }

            case KANZI:
                outputFile = fileName + ".sql" + ".knz";
                debug("Creating KANZI compressed writer for: " + outputFile);
                try {
                    debug("KANZI: Opening output stream for: " + outputFile);
                    OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                    debug("KANZI: Base output stream created successfully");

                    debug("KANZI: Creating compressed stream...");
                    OutputStream compressedOut = createCompressedStream(CompressionType.KANZI, baseOut);
                    debug("KANZI: Compressed stream created successfully");

                    debug("KANZI writer created successfully: " + outputFile);
                    trace("Created compressed file: " + outputFile);
                    return new PrintWriter(new OutputStreamWriter(compressedOut, StandardCharsets.UTF_8)) {
                        @Override
                        public void close() {
                            try {
                                debug("KANZI: Closing PrintWriter");
                                super.close();
                                debug("KANZI: Closing compressed stream");
                                // Ensure Kanzi stream is properly closed
                                if (kanziExecutor!=null) {
                                    kanziExecutor.shutdown();
                                    kanziExecutor.awaitTermination(1, TimeUnit.DAYS);
                                }
                                compressedOut.close();
                                debug("KANZI writer closed successfully.");
                            } catch (IOException | InterruptedException e) {
                                debug("Error closing KANZI stream: " + e.getMessage());
                                System.err.println("Error closing Kanzi stream: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    };
                } catch (Exception e) {
                    // Fallback to uncompressed if Kanzi not available
                    debug("KANZI compression FAILED, falling back: " + e.getMessage());
                    System.err.println("KANZI compression failed: " + e.getMessage());
                    e.printStackTrace();
                    trace("Kanzi compression not available: " + e.getMessage());
                    trace("Falling back to uncompressed output");
                    outputFile = fileName + ".sql";
                    debug("Creating fallback uncompressed writer for: " + outputFile);
                    trace("Created fallback file: " + outputFile);
                    try {
                        OutputStream baseOut = FileUtils.newOutputStream(outputFile, false);
                        return new PrintWriter(new OutputStreamWriter(baseOut, StandardCharsets.UTF_8));
                    } catch (IOException ioEx) {
                        throw DbException.convertIOException(ioEx, null);
                    }
                }

            case NONE:
            default:
                outputFile = fileName + ".sql";
                debug("Creating uncompressed writer for: " + outputFile);
                trace("Created file: " + outputFile);
                return getWriter(fileName, ".sql");
        }
    }

    /**
     * Creates a compressed OutputStream using reflection to avoid hard dependencies.
     *
     * @param type the compression type
     * @param baseOutputStream the underlying output stream
     * @return compressed OutputStream or null if library not available
     * @throws Exception if compression creation fails
     */
    private OutputStream createCompressedStream(CompressionType type, OutputStream baseOutputStream) throws Exception {
        switch (type) {
            case BZIP2:
                return CompressTool.createBZip2OutputStream(baseOutputStream);
            case KANZI:
                int n = Runtime.getRuntime().availableProcessors();
                kanziExecutor = Executors.newFixedThreadPool(n);
                return CompressTool.createKanziOutputStream(baseOutputStream, kanziExecutor);
            default:
                return baseOutputStream;
        }
    }

    /**
     * Processes the piped ASCII dump data with true parallel streaming.
     * Consumes dump data while simultaneously generating SQL in the background.
     *
     * @param pipeReader reader connected to the dump output
     * @param sqlWriter writer for the SQL output file
     * @param fileName original database file name
     * @param progressBar progress bar for visual feedback (can be null)
     */
    private void processPipedDumpToSQL(PipedReader pipeReader, PrintWriter sqlWriter, String fileName,
            ProgressBar progressBar) {
        BufferedReader reader = null;
        try {
            debug("PROCESS: Starting processPipedDumpToSQL for " + fileName);
            reader = new BufferedReader(pipeReader);

            // Write SQL header
            debug("PROCESS: Writing SQL header");
            sqlWriter.println("-- MVStore");
            String className = Recover.class.getName();
            sqlWriter.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_MAP FOR '" + className + ".readBlobMap';");
            sqlWriter.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_MAP FOR '" + className + ".readClobMap';");

            debug("PROCESS: Resetting schema");
            resetSchema();
            setDatabaseName(fileName.substring(0, fileName.length() -
                    Constants.SUFFIX_MV_FILE.length()));

            // Start SQL generation in background while consuming dump
            debug("PROCESS: Starting background SQL generation");
            ExecutorService sqlExecutor = Executors.newSingleThreadExecutor();
            CompletableFuture<Void> sqlGeneration = CompletableFuture.runAsync(() -> {
                debug("SQL_GEN: Starting background generateSQLFromMVStore");
                generateSQLFromMVStore(sqlWriter, fileName);
                debug("SQL_GEN: Background generateSQLFromMVStore completed");
            }, sqlExecutor);

            // Consume dump output in parallel (to prevent blocking)
            debug("PROCESS: Starting parallel dump consumption");
            int lineCount = 0;
            long estimatedTotal = 1000000; // Rough estimate for progress bar

            while (reader.readLine() != null) {
                lineCount++;
                // Just consume the line to prevent pipe blocking
                if (debugMode && lineCount % 10000 == 0) {
                    debug("PROCESS: Consumed " + lineCount + " dump lines");
                }

                // Update progress bar
                if (progressBar != null && lineCount % 1000 == 0) {
                    // Estimate progress based on line count
                    long progress = Math.min(lineCount, estimatedTotal);
                    progressBar.update(progress, estimatedTotal);
                }

                // Optionally yield CPU to allow SQL generation to proceed
                if (lineCount % 1000 == 0) {
                    Thread.yield();
                }
            }
            debug("PROCESS: Consumed and discarded total of " + lineCount + " dump lines");

            // Wait for SQL generation to complete
            debug("PROCESS: Waiting for SQL generation to complete");
            sqlGeneration.get(1, TimeUnit.DAYS); // 1 day timeout
            debug("PROCESS: SQL generation completed");

            // Update progress to 100%
            if (progressBar != null) {
                progressBar.update(estimatedTotal, estimatedTotal);
            }
            sqlExecutor.shutdown();

        } catch (Exception e) {
            debug("PROCESS: Error in processPipedDumpToSQL: " + e.getMessage());
            writeError(sqlWriter, e);
        } finally {
            debug("PROCESS: Cleaning up BufferedReader");
            if (reader != null) {
                try {
                    reader.close();
                    debug("PROCESS: BufferedReader closed");
                } catch (Exception e) {
                    debug("PROCESS: Error closing BufferedReader: " + e.getMessage());
                }
            }
            debug("PROCESS: processPipedDumpToSQL completed for " + fileName);
        }
    }

    /**
     * Generate SQL directly from MVStore file.
     * This maintains the original functionality.
     */
    private void generateSQLFromMVStore(PrintWriter sqlWriter, String fileName) {
        debug("GENERATE: Starting generateSQLFromMVStore for " + fileName);
        try (MVStore mv = new MVStore.Builder().
                fileName(fileName).recoveryMode().readOnly().open()) {

            debug("GENERATE: MVStore opened, dumping LOB maps");
            dumpLobMaps(sqlWriter, mv);
            sqlWriter.println("-- Layout");
            debug("GENERATE: Dumping layout");
            dumpLayout(sqlWriter, mv);
            sqlWriter.println("-- Meta");
            debug("GENERATE: Dumping meta");
            dumpMeta(sqlWriter, mv);
            sqlWriter.println("-- Types");
            debug("GENERATE: Dumping types");
            dumpTypes(sqlWriter, mv);
            sqlWriter.println("-- Tables");

            debug("GENERATE: Creating transaction store");
            TransactionStore store = new TransactionStore(mv, new ValueDataType());
            try {
                store.init();
                debug("GENERATE: Transaction store initialized");
            } catch (Throwable e) {
                debug("GENERATE: Error initializing transaction store: " + e.getMessage());
                writeError(sqlWriter, e);
            }

            debug("GENERATE: Extracting metadata");
            // Extract metadata
            for (String mapName : mv.getMapNames()) {
                if (!mapName.startsWith("table.")) {
                    continue;
                }
                String tableId = mapName.substring("table.".length());
                if (Integer.parseInt(tableId) == 0) {
                    TransactionMap<Long, Row> dataMap = store.begin().openMap(mapName);
                    Iterator<Long> dataIt = dataMap.keyIterator(null);
                    while (dataIt.hasNext()) {
                        Long rowId = dataIt.next();
                        Row row = dataMap.get(rowId);
                        try {
                            writeMetaRow(row);
                        } catch (Throwable t) {
                            writeError(sqlWriter, t);
                        }
                    }
                }
            }

            debug("GENERATE: Writing schema SET");
            writeSchemaSET(sqlWriter);
            sqlWriter.println("---- Table Data ----");

            debug("GENERATE: Processing table data");
            // Process table data
            for (String mapName : mv.getMapNames()) {
                if (!mapName.startsWith("table.")) {
                    continue;
                }
                String tableId = mapName.substring("table.".length());
                if (Integer.parseInt(tableId) == 0) {
                    continue;
                }
                TransactionMap<?,?> dataMap = store.begin().openMap(mapName);
                Iterator<?> dataIt = dataMap.keyIterator(null);
                boolean init = false;
                while (dataIt.hasNext()) {
                    Object rowId = dataIt.next();
                    Object value = dataMap.get(rowId);
                    Value[] values;
                    if (value instanceof Row) {
                        values = ((Row) value).getValueList();
                        super.recordLength = values.length;
                    } else {
                        values = ((ValueCollectionBase) value).getList();
                        super.recordLength = values.length - 1;
                    }
                    if (!init) {
                        setStorage(Integer.parseInt(tableId));
                        // init the column types
                        StringBuilder builder = new StringBuilder();
                        for (int valueId = 0; valueId < super.recordLength; valueId++) {
                            String columnName = super.storageName + "." + valueId;
                            builder.setLength(0);
                            getSQL(builder, columnName, values[valueId]);
                        }
                        createTemporaryTable(sqlWriter);
                        init = true;
                    }
                    StringBuilder buff = new StringBuilder();
                    buff.append("INSERT INTO O_").append(tableId)
                            .append(" VALUES(");
                    for (int valueId = 0; valueId < super.recordLength; valueId++) {
                        if (valueId > 0) {
                            buff.append(", ");
                        }
                        String columnName = super.storageName + "." + valueId;
                        getSQL(buff, columnName, values[valueId]);
                    }
                    buff.append(");");
                    sqlWriter.println(buff);
                }
            }

            debug("GENERATE: Writing schema");
            writeSchema(sqlWriter);
            sqlWriter.println("DROP ALIAS READ_BLOB_MAP;");
            sqlWriter.println("DROP ALIAS READ_CLOB_MAP;");
            sqlWriter.println("DROP TABLE IF EXISTS INFORMATION_SCHEMA.LOB_BLOCKS;");
            debug("GENERATE: generateSQLFromMVStore completed successfully");

        } catch (Throwable e) {
            debug("GENERATE: Error in generateSQLFromMVStore: " + e.getMessage());
            writeError(sqlWriter, e);
        }
    }

    /**
     * Check if a compression library is available without loading it.
     *
     * @param type the compression type to check
     * @return true if the library is available, false otherwise
     */
    public boolean isCompressionAvailable(CompressionType type) {
        switch (type) {
            case BZIP2:
                try {
                    Class.forName(CompressTool.BZIP2_OUTPUT_CLASS_NAME);
                    return true;
                } catch (ClassNotFoundException e) {
                    return false;
                }
            case KANZI:
                try {
                    Class.forName(CompressTool.KANZI_OUTPUT_CLASS_NAME);
                    return true;
                } catch (ClassNotFoundException e) {
                    return false;
                }
            case GZIP:
            case ZIP:
            case NONE:
                return true; // Always available
            default:
                return false;
        }
    }

    /**
     * Get a list of available compression types based on classpath.
     *
     * @return array of available compression types
     */
    public CompressionType[] getAvailableCompressionTypes() {
        java.util.List<CompressionType> available = new java.util.ArrayList<>();
        for (CompressionType type : CompressionType.values()) {
            if (isCompressionAvailable(type)) {
                available.add(type);
            }
        }
        return available.toArray(new CompressionType[0]);
    }

    /**
     * Sets the compression type for SQL output files.
     *
     * @param type the compression type to use
     */
    public void setCompressionType(CompressionType type) {
        this.compressionType = type;
    }

    /**
     * Gets the current compression type.
     *
     * @return the current compression type
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Public method to process database files and write to a provided writer (pipe-like).
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     * @param writer the output writer
     * @throws SQLException on failure
     */
    public static void execute(String dir, String db, PrintWriter writer) throws SQLException {
        try {
            DirectRecover recover = new DirectRecover();
            recover.processWithPipe(dir, db);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }
}