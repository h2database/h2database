/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.text.Collator;

import org.h2.command.Prepared;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.tools.CompressTool;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.ValueInt;

/**
 * @author Thomas
 */

public class Set extends Prepared {

    private int type;
    private Expression expression;
    private String stringValue;

    public Set(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    public boolean isTransactional() {
        return false;
    }

    public int update() throws SQLException {
        // Value v = expr.getValue();
        Database database = session.getDatabase();
        String name = SetTypes.getTypeName(type);
        switch (type) {
        case SetTypes.MAX_LOG_SIZE:
            session.getUser().checkAdmin();
            session.getDatabase().setMaxLogSize((long)getIntValue() * 1024 * 1024);
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.LOCK_TIMEOUT:
            session.setLockTimeout(getIntValue());
            break;
        case SetTypes.LOCK_MODE:
            session.getUser().checkAdmin();
            database.setLockMode(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.DEFAULT_LOCK_TIMEOUT:
            session.getUser().checkAdmin();
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.DEFAULT_TABLE_TYPE:
            session.getUser().checkAdmin();
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            session.getUser().checkAdmin();
            database.getTraceSystem().setLevelSystemOut(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.TRACE_LEVEL_FILE:
            session.getUser().checkAdmin();
            database.getTraceSystem().setLevelFile(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.TRACE_MAX_FILE_SIZE: {
            session.getUser().checkAdmin();
            int size = getIntValue() * 1024 * 1024;
            database.getTraceSystem().setMaxFileSize(size);
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.CACHE_SIZE:
            session.getUser().checkAdmin();
            database.setCacheSize(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.MODE:
            session.getUser().checkAdmin();
            Mode mode = Mode.getMode(stringValue);
            if(mode == null) {
                throw Message.getSQLException(Message.UNKNOWN_MODE_1, stringValue);
            }
            Mode.setCurrentMode(mode);
            break;
        case SetTypes.COLLATION: {
            session.getUser().checkAdmin();
            ObjectArray array = database.getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
            for(int i=0; i<array.size(); i++) {
                Table table = (Table) array.get(i);
                if(table.getCreateSQL() != null) {
                    throw Message.getSQLException(Message.COLLATION_CHANGE_WITH_DATA_TABLE_1, table.getSQL());
                }
            }
            CompareMode compareMode;
            StringBuffer buff = new StringBuffer(stringValue);
            if(stringValue.equals(CompareMode.OFF)) {
                compareMode = new CompareMode(null, null);
            } else {
                Collator coll = CompareMode.getCollator(stringValue);
                compareMode = new CompareMode(coll, stringValue);
                buff.append(" STRENGTH ");
                if(getIntValue() == Collator.IDENTICAL) {
                    buff.append("IDENTICAL");
                } else if(getIntValue() == Collator.PRIMARY) {
                    buff.append("PRIMARY");
                } else if(getIntValue() == Collator.SECONDARY) {
                    buff.append("SECONDARY");
                } else if(getIntValue() == Collator.TERTIARY) {
                    buff.append("TERTIARY");
                }
                coll.setStrength(getIntValue());
            }
            addOrUpdateSetting(name, buff.toString(), 0);
            database.setCompareMode(compareMode);
            break;
        }
        case SetTypes.IGNORECASE:
            session.getUser().checkAdmin();
            session.getDatabase().setIgnoreCase(getIntValue() == 1);
            addOrUpdateSetting(name, null, getIntValue());
            break;
        case SetTypes.CLUSTER: {
            session.getUser().checkAdmin();
            database.setCluster(StringUtils.quoteStringSQL(stringValue));
            addOrUpdateSetting(name, StringUtils.quoteStringSQL(stringValue), 0);
            break;
        }
        case SetTypes.WRITE_DELAY: {
            session.getUser().checkAdmin();
            database.setWriteDelay(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.DATABASE_EVENT_LISTENER: {
            session.getUser().checkAdmin();
            database.setEventListener(stringValue);
            break;
        }
        case SetTypes.MAX_MEMORY_ROWS: {
            session.getUser().checkAdmin();
            database.setMaxMemoryRows(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.MULTI_THREADED: {
            session.getUser().checkAdmin();
            Constants.MULTI_THREADED_KERNEL = (getIntValue() == 1);
            break;
        }
        case SetTypes.DB_CLOSE_DELAY: {
            session.getUser().checkAdmin();
            database.setCloseDelay(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.LOG: {
            int value = getIntValue();
            if(value<0 || value>2) {
                throw Message.getInvalidValueException(""+getIntValue(), "LOG");
            }
            if(value==0) {
                session.getUser().checkAdmin();
            }
            database.setLog(value);
            break;
        }
        case SetTypes.THROTTLE: {
            if(getIntValue() < 0) {
                throw Message.getInvalidValueException(""+getIntValue(), "THROTTLE");
            }
            session.setThrottle(getIntValue());
            break;
        }
        case SetTypes.MAX_MEMORY_UNDO: {
            if(getIntValue() < 0) {
                throw Message.getInvalidValueException(""+getIntValue(), "MAX_MEMORY_UNDO");
            }
            session.getUser().checkAdmin();
            database.setMaxMemoryUndo(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.MAX_LENGTH_INPLACE_LOB: {
            if(getIntValue() < 0) {
                throw Message.getInvalidValueException(""+getIntValue(), "MAX_LENGTH_INPLACE_LOB");
            }
            session.getUser().checkAdmin();
            database.setMaxLengthInplaceLob(getIntValue());
            addOrUpdateSetting(name, null, getIntValue());
            break;
        }
        case SetTypes.COMPRESS_LOB: {
            session.getUser().checkAdmin();
            int algo = CompressTool.getInstance().getCompressAlgorithm(stringValue);
            database.setLobCompressionAlgorithm(algo == Compressor.NO ? null : stringValue);
            addOrUpdateSetting(name, stringValue, 0);
            break;
        }
        case SetTypes.ALLOW_LITERALS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if(value < 0 || value > 2) {
                throw Message.getInvalidValueException(""+getIntValue(), "ALLOW_LITERALS");
            }
            database.setAllowLiterals(value);
            addOrUpdateSetting(name, null, value);
            break;
        }
        case SetTypes.SCHEMA: {
            Schema schema = database.getSchema(stringValue);
            session.setCurrentSchema(schema);
            break;
        }
        case SetTypes.OPTIMIZE_REUSE_RESULTS: {
            session.getUser().checkAdmin();
            database.setOptimizeReuseResults(getIntValue() != 0);
            break;
        }
        default:
            throw Message.getInternalError("type="+type);
        }
        return 0;
    }

    private int getIntValue() throws SQLException {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    private void addOrUpdateSetting(String name, String s, int v) throws SQLException {
        Database database = session.getDatabase();
        if(database.getReadOnly()) {
            return;
        }
        Setting setting = database.findSetting(name);
        boolean addNew = false;
        if(setting == null) {
            addNew = true;
            int id = getObjectId(false, true);
            setting = new Setting(database, id, name);
        }
        if(s != null) {
            if(!addNew && setting.getStringValue().equals(s)) {
                return;
            }
            setting.setStringValue(s);
        } else {
            if(!addNew && setting.getIntValue() == v) {
                return;
            }
            setting.setIntValue(v);
        }
        if(addNew) {
            database.addDatabaseObject(session, setting);
        } else {
            database.update(session, setting);
        }
    }

    public boolean needRecompile() {
        return false;
    }

    public LocalResult queryMeta() {
        return null;
    }

}
