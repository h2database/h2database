package org.h2.constant;

import org.h2.engine.Constants;
/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
import org.h2.message.TraceSystem;

/**
 * The constants defined in this class are initialized from system properties.
 * Those properties can be set when starting the virtual machine:
 * <pre>
 * java -Dh2.baseDir=/temp
 * </pre>
 * They can be set within the application, but this must be done before loading any classes of this database
 * (before loading the JDBC driver):
 * <pre>
 * System.setProperty("h2.baseDir", "/temp");
 * </pre>
 */
public class SysProperties {

    public static final int MIN_WRITE_DELAY = SysProperties.getIntSetting("h2.minWriteDelay", 5);
    public static final boolean CHECK = SysProperties.getBooleanSetting("h2.check", true);
    public static final boolean CHECK2 = SysProperties.getBooleanSetting("h2.check2", false);
    public static final boolean OPTIMIZE_MIN_MAX = SysProperties.getBooleanSetting("h2.optimizeMinMax", true);
    public static final boolean OPTIMIZE_IN = SysProperties.getBooleanSetting("h2.optimizeIn", true);
    public static final int REDO_BUFFER_SIZE = SysProperties.getIntSetting("h2.redoBufferSize", 256 * 1024);
    public static final boolean RECOMPILE_ALWAYS = SysProperties.getBooleanSetting("h2.recompileAlways", false);
    public static final boolean OPTIMIZE_SUBQUERY_CACHE = SysProperties.getBooleanSetting("h2.optimizeSubqueryCache", true);
    public static final boolean OVERFLOW_EXCEPTIONS = SysProperties.getBooleanSetting("h2.overflowExceptions", true);
    public static final boolean LOG_ALL_ERRORS = SysProperties.getBooleanSetting("h2.logAllErrors", false);
    public static final String LOG_ALL_ERRORS_FILE = SysProperties.getStringSetting("h2.logAllErrorsFile", "h2errors.txt");
    public static final int SERVER_CACHED_OBJECTS = SysProperties.getIntSetting("h2.serverCachedObjects", 64);
    public static final int SERVER_SMALL_RESULT_SET_SIZE = SysProperties.getIntSetting("h2.serverSmallResultSetSize", 100);
    public static final int EMERGENCY_SPACE_INITIAL = SysProperties.getIntSetting("h2.emergencySpaceInitial", 1 * 1024 * 1024);
    public static final int EMERGENCY_SPACE_MIN = SysProperties.getIntSetting("h2.emergencySpaceMin", 128 * 1024);
    public static final boolean OBJECT_CACHE = SysProperties.getBooleanSetting("h2.objectCache", true);
    public static final int OBJECT_CACHE_SIZE = SysProperties.getIntSetting("h2.objectCacheSize", 1024);
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = SysProperties.getIntSetting("h2.objectCacheMaxPerElementSize", 4096);
    public static final String CLIENT_TRACE_DIRECTORY = SysProperties.getStringSetting("h2.clientTraceDirectory", "trace.db/");
    public static final int MAX_FILE_RETRY = Math.max(1, SysProperties.getIntSetting("h2.maxFileRetry", 16));
    public static final boolean ALLOW_BIG_DECIMAL_EXTENSIONS = SysProperties.getBooleanSetting("h2.allowBigDecimalExtensions", false);
    public static final boolean INDEX_LOOKUP_NEW = SysProperties.getBooleanSetting("h2.indexLookupNew", true);
    public static final boolean TRACE_IO = SysProperties.getBooleanSetting("h2.traceIO", false);
    public static final int DATASOURCE_TRACE_LEVEL = SysProperties.getIntSetting("h2.dataSourceTraceLevel", TraceSystem.ERROR);
    public static final int CACHE_SIZE_DEFAULT = SysProperties.getIntSetting("h2.cacheSizeDefault", 16 * 1024);
    public static final int CACHE_SIZE_INDEX_SHIFT = SysProperties.getIntSetting("h2.cacheSizeIndexShift", 3);
    public static final int DEFAULT_MAX_MEMORY_UNDO = SysProperties.getIntSetting("h2.defaultMaxMemoryUndo", 50000);
    public static final boolean OPTIMIZE_NOT = SysProperties.getBooleanSetting("h2.optimizeNot", true);
    public static final boolean OPTIMIZE_TWO_EQUALS = SysProperties.getBooleanSetting("h2.optimizeTwoEquals", true);
    public static final int DEFAULT_LOCK_MODE = SysProperties.getIntSetting("h2.defaultLockMode", Constants.LOCK_MODE_READ_COMMITTED);
    public static boolean runFinalize = SysProperties.getBooleanSetting("h2.runFinalize", true);
    public static String scriptDirectory = SysProperties.getStringSetting("h2.scriptDirectory", "");
    public static String baseDir = SysProperties.getStringSetting("h2.baseDir", null);
    public static boolean multiThreadedKernel = SysProperties.getBooleanSetting("h2.multiThreadedKernel", false);
    public static boolean lobCloseBetweenReads = SysProperties.getBooleanSetting("h2.lobCloseBetweenReads", false);
    // TODO: also remove DataHandler.allocateObjectId, createTempFile when setting this to true and removing it
    public static final boolean LOB_FILES_IN_DIRECTORIES = SysProperties.getBooleanSetting("h2.lobFilesInDirectories", false);
    public static final int LOB_FILES_PER_DIRECTORY = SysProperties.getIntSetting("h2.lobFilesPerDirectory", 256);
    
    private static boolean getBooleanSetting(String name, boolean defaultValue) {
        String s = System.getProperty(name);
        if(s != null) {
            try {
                return Boolean.valueOf(s).booleanValue();
            } catch(NumberFormatException e) {
            }
        }
        return defaultValue;
    }
    
    private static String getStringSetting(String name, String defaultValue) {
        String s = System.getProperty(name);
        return s == null ? defaultValue : s;
    }
    
    private static int getIntSetting(String name, int defaultValue) {
        String s = System.getProperty(name);
        if(s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch(NumberFormatException e) {
            }
        }
        return defaultValue;
    }
    
    /**
     * INTERNAL
     */
    public static void setBaseDir(String dir) {
        if(!dir.endsWith("/")) {
            dir += "/";
        }
        baseDir = dir;
    }
    
    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        return baseDir;
    }
}
