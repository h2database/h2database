/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.SmallLRUCache;

/**
 * The trace mechanism is the logging facility of this database. There is
 * usually one trace system per database. It is called 'trace' because the term
 * 'log' is already used in the database domain and means 'transaction log'. It
 * is possible to write after close was called, but that means for each write
 * the log file will be opened and closed again (which is slower).
 */
public class TraceSystem implements TraceWriter {
    public static final int OFF = 0, ERROR = 1, INFO = 2, DEBUG = 3;
    public static final int ADAPTER = 4;

    // max file size is currently 64 MB,
    // and then there could be a .old file of the same size
    private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024;
    public static final int DEFAULT_TRACE_LEVEL_SYSTEM_OUT = OFF;
    public static final int DEFAULT_TRACE_LEVEL_FILE = ERROR;
    private static final int CHECK_FILE_TIME = 4000;
    private int levelSystemOut = DEFAULT_TRACE_LEVEL_SYSTEM_OUT;
    private int levelFile = DEFAULT_TRACE_LEVEL_FILE;
    private int maxFileSize = DEFAULT_MAX_FILE_SIZE;
    private String fileName;
    private long lastCheck;
    private SmallLRUCache traces;
    private SimpleDateFormat dateFormat;
    private Writer fileWriter;
    private PrintWriter printWriter;
    private static final int CHECK_SIZE_EACH_WRITES = 128;
    private int checkSize;
    private boolean closed;
    private boolean manualEnabling = true;
    private boolean writingErrorLogged;
    private TraceWriter writer = this;

    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }

    public void setManualEnabling(boolean value) {
        this.manualEnabling = value;
    }

    public TraceSystem(String fileName, boolean init) {
        this.fileName = fileName;
        traces = new SmallLRUCache(100);
        dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss ");
        if (fileName != null && init) {
            try {
                openWriter();
            } catch (Exception e) {
                logWritingError(e);
            }
        }
    }

    public Trace getTrace(String module) {
        Trace t = (Trace) traces.get(module);
        if (t == null) {
            t = new Trace(writer, module);
            traces.put(module, t);
        }
        return t;
    }

    public boolean isEnabled(int level) {
        int max = Math.max(levelSystemOut, levelFile);
        return level <= max;
    }

    public void setFileName(String name) {
        this.fileName = name;
    }

    public void setMaxFileSize(int max) {
        this.maxFileSize = max;
    }

    public void setLevelSystemOut(int level) {
        levelSystemOut = level;
    }

    public void setLevelFile(int level) {
        if (level == ADAPTER) {
            String adapterClass = "org.h2.message.TraceWriterAdapter";
            try {
                writer = (TraceWriter) ClassUtils.loadSystemClass(adapterClass).newInstance();
            } catch (Throwable e) {
                e = Message.getSQLException(ErrorCode.CLASS_NOT_FOUND_1, new String[] { adapterClass }, e);
                write(ERROR, Trace.DATABASE, adapterClass, e);
                return;
            }
            String name = fileName;
            if (name != null) {
                if (name.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    name = name.substring(0, name.length() - Constants.SUFFIX_TRACE_FILE.length());
                }
                int idx = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
                if (idx >= 0) {
                    name = name.substring(idx + 1);
                }
                writer.setName(name);
            }
        }
        levelFile = level;
    }

    private String format(String module, String s) {
        synchronized (dateFormat) {
            return dateFormat.format(new Date()) + module + ": " + s;
        }
    }

    public void write(int level, String module, String s, Throwable t) {
        if (level <= levelSystemOut) {
            System.out.println(format(module, s));
            if (t != null && levelSystemOut == DEBUG) {
                t.printStackTrace();
            }
        }
        if (fileName != null) {
            if (level > levelFile) {
                enableIfRequired();
            }
            if (level <= levelFile) {
                writeFile(format(module, s), t);
            }
        }
    }

    private void enableIfRequired() {
        if (!manualEnabling) {
            return;
        }
        long time = System.currentTimeMillis();
        if (time > lastCheck + CHECK_FILE_TIME) {
            String checkFile = fileName + Constants.SUFFIX_TRACE_START_FILE;
            lastCheck = time;
            if (FileUtils.exists(checkFile)) {
                levelFile = DEBUG;
                try {
                    FileUtils.delete(checkFile);
                } catch (Exception e) {
                    // the file may be read only
                }
            }
        }
    }

    private synchronized void writeFile(String s, Throwable t) {
        try {
            if (checkSize++ >= CHECK_SIZE_EACH_WRITES) {
                checkSize = 0;
                closeWriter();
                if (maxFileSize > 0 && FileUtils.length(fileName) > maxFileSize) {
                    String old = fileName + ".old";
                    if (FileUtils.exists(old)) {
                        FileUtils.delete(old);
                    }
                    FileUtils.rename(fileName, old);
                }
            }
            if (!openWriter()) {
                return;
            }
            printWriter.println(s);
            if (t != null) {
                t.printStackTrace(printWriter);
            }
            printWriter.flush();
            if (closed) {
                closeWriter();
            }
        } catch (Exception e) {
            logWritingError(e);
        }
    }

    private void logWritingError(Exception e) {
        if (writingErrorLogged) {
            return;
        }
        writingErrorLogged = true;
        SQLException se = Message.getSQLException(ErrorCode.TRACE_FILE_ERROR_2, new String[] { fileName, e.toString() },
                e);
        // print this error only once
        fileName = null;
        System.out.println(se);
        se.printStackTrace();
    }

    private boolean openWriter() {
        if (printWriter == null) {
            try {
                FileUtils.createDirs(fileName);
                if (FileUtils.exists(fileName) && FileUtils.isReadOnly(fileName)) {
                    // read only database: don't log error if the trace file
                    // can't be opened
                    return false;
                }
                fileWriter = FileUtils.openFileWriter(fileName, true);
                printWriter = new PrintWriter(fileWriter, true);
            } catch (Exception e) {
                logWritingError(e);
                return false;
            }
        }
        return true;
    }

    private synchronized void closeWriter() {
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
            printWriter = null;
        }
        if (fileWriter != null) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                // ignore
            }
            fileWriter = null;
        }
        try {
            if (fileName != null && FileUtils.length(fileName) == 0) {
                FileUtils.delete(fileName);
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    public void close() {
        closeWriter();
        closed = true;
    }

    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        close();
    }

    public void setName(String name) {
    }

}
