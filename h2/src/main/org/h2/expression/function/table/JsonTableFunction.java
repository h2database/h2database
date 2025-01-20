/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.table;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map.Entry;

import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.function.json.JsonEncoding;
import org.h2.expression.function.json.JsonFunction;
import org.h2.expression.function.json.JsonQuery;
import org.h2.expression.function.json.QueryQuotesBehavior;
import org.h2.expression.function.json.QueryWrapperBehavior;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;
import org.h2.util.json.path.JsonPath;
import org.h2.value.TypeInfo;

/**
 * JSON table function.
 */
public class JsonTableFunction extends TableFunction {

    public static abstract class AbstractJsonColumn implements HasSQL {

        int columnIndex;

        AbstractJsonColumn() {
        }

        abstract void fillColumns(ArrayList<Column> columns, ArrayList<NestedJsonColumns> nestedColumns,
                BitSet ordinalityColumns);

        abstract void optimize(SessionLocal session);

        abstract void compile(SessionLocal session, JsonPath[] paths);

    }

    public static final class OrdinalityJsonColumn extends AbstractJsonColumn {

        private final String name;

        public OrdinalityJsonColumn(String name) {
            this.name = name;
        }

        @Override
        void fillColumns(ArrayList<Column> columns, ArrayList<NestedJsonColumns> nestedColumns,
                BitSet ordinalityColumns) {
            columnIndex = columns.size();
            columns.add(new Column(name, TypeInfo.TYPE_BIGINT));
            ordinalityColumns.set(columnIndex);
        }

        @Override
        void optimize(SessionLocal session) {
        }

        @Override
        void compile(SessionLocal session, JsonPath[] paths) {
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            return StringUtils.quoteIdentifier(builder, name).append(" FOR ORDINALITY");
        }

    }

    public static final class JsonColumn extends AbstractJsonColumn implements JsonQuery {

        private final String name;

        private Expression path;

        private TypeInfo type;

        private boolean formatted;

        private JsonEncoding encoding;

        private QueryWrapperBehavior queryWrapperBehavior;

        private QueryQuotesBehavior queryQuotesBehavior;

        private Expression onEmpty;

        private Expression onError;

        private JsonPath jsonPath;

        public JsonColumn(String name) {
            this.name = name;
        }

        public void setPath(Expression path) {
            this.path = path;
        }

        public void setType(TypeInfo type, JsonEncoding encoding) {
            this.type = type;
            this.encoding = encoding;
        }

        public void setFormatted(boolean formatted) {
            this.formatted = formatted;
        }

        @Override
        public void setQueryWrapperBehavior(QueryWrapperBehavior queryWrapperBehavior) {
            this.queryWrapperBehavior = queryWrapperBehavior;
        }

        @Override
        public void setQueryQuotesBehavior(QueryQuotesBehavior queryQuotesBehavior) {
            this.queryQuotesBehavior = queryQuotesBehavior;
        }

        @Override
        public void setOnEmpty(Expression onEmpty) {
            this.onEmpty = onEmpty;
        }

        @Override
        public void setOnError(Expression onError) {
            this.onError = onError;
        }

        @Override
        void fillColumns(ArrayList<Column> columns, ArrayList<NestedJsonColumns> nestedColumns,
                BitSet ordinalityColumns) {
            columnIndex = columns.size();
            columns.add(new Column(name, type));
        }

        @Override
        void optimize(SessionLocal session) {
            if (path != null) {
                path = path.optimize(session);
                if (path.isConstant()) {
                    String pathString = path.getValue(session).getString();
                    if (pathString != null) {
                        jsonPath = JsonPath.get(pathString);
                    }
                }
            } else {
                jsonPath = JsonPath.getMemberAccessor(name);
            }
        }

        @Override
        void compile(SessionLocal session, JsonPath[] paths) {
            JsonPath jsonPath = this.jsonPath;
            if (jsonPath == null) {
                String pathString = path.getValue(session).getString();
                if (pathString != null) {
                    jsonPath = JsonPath.get(pathString);
                }
            }
            paths[columnIndex] = jsonPath;
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            StringUtils.quoteIdentifier(builder, name);
            type.getSQL(builder.append(' '), sqlFlags);
            if (formatted) {
                builder.append(" FORMAT JSON");
                if (encoding != null) {
                    builder.append(" ENCODING ").append(encoding.name());
                }
                if (path != null) {
                    path.getUnenclosedSQL(builder.append(" PATH "), sqlFlags);
                }
                JsonFunction.addJsonQuerySQL(builder, queryWrapperBehavior, queryQuotesBehavior, onEmpty, onError);
            } else {
                if (path != null) {
                    path.getUnenclosedSQL(builder.append(" PATH "), sqlFlags);
                }
                JsonFunction.addJsonValueOn(builder, sqlFlags, onEmpty, "ON EMPTY");
                JsonFunction.addJsonValueOn(builder, sqlFlags, onError, "ON ERROR");
            }
            return builder;
        }

    }

    public static final class NestedJsonColumns extends AbstractJsonColumn {

        private Expression path;

        private final String pathAlias;

        private final ArrayList<AbstractJsonColumn> jsonColumns;

        private JsonPath jsonPath;

        public NestedJsonColumns(Expression path, String pathAlias, ArrayList<AbstractJsonColumn> jsonColumns) {
            this.path = path;
            this.pathAlias = pathAlias;
            this.jsonColumns = jsonColumns;
        }

        @Override
        void fillColumns(ArrayList<Column> columns, ArrayList<NestedJsonColumns> nestedColumns,
                BitSet ordinalityColumns) {
            for (AbstractJsonColumn column : jsonColumns) {
                column.fillColumns(columns, nestedColumns, ordinalityColumns);
            }
            nestedColumns.add(this);
        }

        @Override
        void optimize(SessionLocal session) {
            path = path.optimize(session);
            if (path.isConstant()) {
                String pathString = path.getValue(session).getString();
                if (pathString != null) {
                    jsonPath = JsonPath.get(pathString);
                }
            }
            for (AbstractJsonColumn column : jsonColumns) {
                column.optimize(session);
            }
        }

        @Override
        void compile(SessionLocal session, JsonPath[] paths) {
            JsonPath jsonPath = this.jsonPath;
            if (jsonPath == null) {
                String pathString = path.getValue(session).getString();
                if (pathString != null) {
                    jsonPath = JsonPath.get(pathString);
                }
            }
            paths[columnIndex] = jsonPath;
            for (AbstractJsonColumn column : jsonColumns) {
                column.compile(session, paths);
            }
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            path.getUnenclosedSQL(builder.append("NESTED PATH "), sqlFlags);
            if (pathAlias != null) {
                StringUtils.quoteIdentifier(builder.append(" AS "), pathAlias);
            }
            return getColumnsSQL(builder, sqlFlags);
        }

        private StringBuilder getColumnsSQL(StringBuilder builder, int sqlFlags) {
            builder.append("COLUMNS (");
            boolean f = false;
            for (AbstractJsonColumn column : jsonColumns) {
                if (f) {
                    builder.append(", ");
                }
                f = true;
                column.getSQL(builder, sqlFlags);
            }
            return builder.append(')');
        }

    }

    private NestedJsonColumns jsonColumns;

    private final HashMap<String, Expression> passing;

    private boolean errorOnError;

    private Column[] columns;

    private int pathsCount;

    private BitSet ordinalityColumns;

    public JsonTableFunction(Expression arg, Expression path, String pathAlias, HashMap<String, Expression> passing) {
        super(new Expression[] { arg });
        jsonColumns = new NestedJsonColumns(path, pathAlias, new ArrayList<>());
        this.passing = passing;
    }

    public void setJsonColumns(ArrayList<AbstractJsonColumn> jsonColumns) {
        this.jsonColumns.jsonColumns.addAll(jsonColumns);
        ArrayList<Column> columns = new ArrayList<>();
        ArrayList<NestedJsonColumns> nestedColumns = new ArrayList<>();
        BitSet ordinalityColumns = new BitSet();
        this.jsonColumns.fillColumns(columns, nestedColumns, ordinalityColumns);
        int pathsCount = columns.size();
        for (NestedJsonColumns nested : nestedColumns) {
            nested.columnIndex = pathsCount++;
        }
        this.columns = columns.toArray(new Column[0]);
        this.pathsCount = pathsCount;
        this.ordinalityColumns = ordinalityColumns;
    }

    public void setErrorOnError(boolean errorOnError) {
        this.errorOnError = errorOnError;
    }

    @Override
    public void optimize(SessionLocal session) {
        super.optimize(session);
        jsonColumns.optimize(session);
        if (passing != null) {
            for (Entry<String, Expression> entry : passing.entrySet()) {
                entry.setValue(entry.getValue().optimize(session));
            }
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        args[0].getUnenclosedSQL(builder.append(getName()).append('('), sqlFlags).append(", ");
        jsonColumns.path.getUnenclosedSQL(builder, sqlFlags);
        String pathAlias = jsonColumns.pathAlias;
        if (pathAlias != null) {
            StringUtils.quoteIdentifier(builder.append(" AS "), pathAlias);
        }
        if (passing != null) {
            builder.append(" PASSING ");
            boolean f = false;
            for (Entry<String, Expression> entry : passing.entrySet()) {
                if (f) {
                    builder.append(", ");
                }
                f = true;
                entry.getValue().getUnenclosedSQL(builder, sqlFlags);
                StringUtils.quoteIdentifier(builder.append(" AS "), entry.getKey());
            }
        }
        jsonColumns.getColumnsSQL(builder.append(' '), sqlFlags);
        if (errorOnError) {
            builder.append(" ERROR ON ERROR");
        }
        return builder.append(')');
    }

    @Override
    public String getName() {
        return "JSON_TABLE";
    }

    @Override
    public ResultInterface getValue(SessionLocal session) {
        return getTable(session, false);
    }

    @Override
    public ResultInterface getValueTemplate(SessionLocal session) {
        return getTable(session, true);
    }

    private ResultInterface getTable(SessionLocal session, boolean onlyColumnList) {
        int totalColumns = columns.length;
        Expression[] header = new Expression[totalColumns];
        Database db = session.getDatabase();
        for (int i = 0; i < totalColumns; i++) {
            Column c = columns[i];
            ExpressionColumn col = new ExpressionColumn(db, c);
            header[i] = col;
        }
        LocalResult result = new LocalResult(session, header, totalColumns, totalColumns);
        if (!onlyColumnList) {
            generateRows(session, result, totalColumns);
        }
        result.done();
        return result;
    }

    private void generateRows(SessionLocal session, LocalResult result, int totalColumns) {
        throw DbException.getUnsupportedException("JSON_TABLE");
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

}
