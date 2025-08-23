/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.expression.function.Function2;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.util.json.path.JSONDatetime;
import org.h2.util.json.path.JsonPath;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * A JSON function.
 */
public class JsonFunction extends Function2 implements JsonQuery {

    /**
     * JSON_EXISTS().
     */
    public static final int JSON_EXISTS = 0;

    /**
     * JSON_QUERY().
     */
    public static final int JSON_QUERY = JSON_EXISTS + 1;

    /**
     * JSON_VALUE().
     */
    public static final int JSON_VALUE = JSON_QUERY + 1;

    private static final String[] NAMES = { //
            "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE" //
    };

    private final int function;

    private JsonPath path;

    private final HashMap<String, Expression> passing;

    private JsonEncoding encoding;

    private QueryWrapperBehavior queryWrapperBehavior;

    private QueryQuotesBehavior queryQuotesBehavior;

    private Expression onEmpty;

    private Expression onError;

    public JsonFunction(Expression context, Expression pathSource, int function, HashMap<String, Expression> passing) {
        super(context, pathSource);
        this.function = function;
        this.passing = passing;
    }

    public void setFormat(TypeInfo type, JsonEncoding encoding) {
        this.type = type;
        this.encoding = encoding;
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
    public String getName() {
        return NAMES[function];
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = left.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        JSONValue context = v.convertToJson(TypeInfo.TYPE_JSON, Value.CONVERT_TO, null).getDecomposition();
        Map<String, JSONValue> params = convertParams(session, passing);
        JsonPath path = this.path;
        if (path == null) {
            String pathString = right.getValue(session).getString();
            if (pathString == null) {
                return ValueNull.INSTANCE;
            }
            path = JsonPath.get(pathString);
        }
        if (function == JSON_EXISTS) {
            return jsonExists(session, path, context, params);
        }
        switch (function) {
        case JSON_QUERY:
            return jsonQuery(session, path, context, params);
        case JSON_VALUE:
            return jsonValue(session, path, context, params);
        default:
            throw DbException.getUnsupportedException(getName());
        }
    }

    private Value jsonExists(SessionLocal session, JsonPath path, JSONValue context, Map<String, JSONValue> params) {
        try {
            return ValueBoolean.get(!path.execute(session, context, params).isEmpty());
        } catch (DbException e) {
            if (onError == null) {
                throw e;
            }
            return onError.getValue(session).convertTo(Value.BOOLEAN);
        }
    }

    private Value jsonQuery(SessionLocal session, JsonPath path, JSONValue context, Map<String, JSONValue> params) {
        Value v;
        try {
            v = jsonQuery1(session, path, context, params);
        } catch (DbException e) {
            if (onError == null) {
                throw e;
            }
            v = onError.getValue(session);
        }
        if (v == null) {
            if (onEmpty == null) {
                throw DbException.getInvalidValueException("JSON_QUERY result", "empty");
            }
            v = onEmpty.getValue(session);
        }
        if (v != ValueNull.INSTANCE) {
            if (DataType.isBinaryStringType(type.getValueType()) && encoding != null //
                    && encoding != JsonEncoding.UTF8) {
                v = ValueVarbinary.getNoCopy(v.getString().getBytes(encoding.getCharset()));
            }
            v = v.castTo(type, session);
        }
        return v;
    }

    private Value jsonQuery1(SessionLocal session, JsonPath path, JSONValue context, Map<String, JSONValue> params) {
        List<JSONValue> list = path.execute(session, context, params);
        switch (list.size()) {
        case 0:
            return null;
        case 1: {
            JSONValue r = list.get(0);
            if (queryWrapperBehavior == QueryWrapperBehavior.WITH_UNCONDITIONAL) {
                JSONArray array = new JSONArray();
                array.addElement(r);
                r = array;
            } else if (queryQuotesBehavior == QueryQuotesBehavior.OMIT && r instanceof JSONString) {
                return ValueJson.fromJson(((JSONString) r).getString());
            }
            return ValueJson.fromJson(r);
        }
        default: {
            if (queryWrapperBehavior == QueryWrapperBehavior.WITHOUT) {
                throw DbException.getInvalidValueException("JSON_QUERY result", "multiple values");
            }
            JSONArray array = new JSONArray();
            list.forEach(array::addElement);
            return ValueJson.fromJson(array);
        }
        }
    }

    private Value jsonValue(SessionLocal session, JsonPath path, JSONValue context, Map<String, JSONValue> params) {
        Value v = jsonValue1(session, path, context, params);
        try {
            return v.castTo(type, session);
        } catch (DbException e) {
            if (onError == null) {
                throw e;
            }
            return onError.getValue(session);
        }
    }

    private Value jsonValue1(SessionLocal session, JsonPath path, JSONValue context, Map<String, JSONValue> params) {
        List<JSONValue> list;
        try {
            list = path.execute(session, context, params);
        } catch (DbException e) {
            if (onError == null) {
                throw e;
            }
            return onError.getValue(session);
        }
        switch (list.size()) {
        case 0:
            if (onEmpty != null) {
                return onEmpty.getValue(session);
            }
            throw DbException.getInvalidValueException("JSON_VALUE result", "empty");
        case 1: {
            JSONValue r = list.get(0);
            if (r == JSONNull.NULL) {
                return ValueNull.INSTANCE;
            } else {
                int t = type.getValueType();
                try {
                    if (DataType.isCharacterStringType(t)) {
                        if (r instanceof JSONString) {
                            return ValueVarchar.get(((JSONString) r).getString(), session);
                        }
                    } else if (t == Value.BOOLEAN) {
                        if (r instanceof JSONBoolean) {
                            return ValueBoolean.get(((JSONBoolean) r).getBoolean());
                        }
                    } else if (DataType.isNumericType(t)) {
                        if (r instanceof JSONNumber) {
                            return ValueNumeric.get(((JSONNumber) r).getBigDecimal());
                        }
                    } else if (DataType.isDateTimeType(t)) {
                        if (r instanceof JSONDatetime) {
                            return ((JSONDatetime) r).getValue();
                        }
                    }
                    return ValueJson.fromJson(r);
                } catch (DbException e) {
                    if (onError == null) {
                        throw e;
                    }
                    return onError.getValue(session);
                }
            }
        }
        default:
            if (onError != null) {
                return onError.getValue(session);
            } else {
                throw DbException.getInvalidValueException("JSON_VALUE result", "multiple values");
            }
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (passing != null) {
            for (Entry<String, Expression> entry : passing.entrySet()) {
                entry.setValue(entry.getValue().optimize(session));
            }
        }
        switch (function) {
        case JSON_EXISTS:
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        case JSON_QUERY:
            if (type == null) {
                type = TypeInfo.TYPE_JSON;
            } else {
                int t = type.getValueType();
                if (type.getValueType() != Value.JSON && !DataType.isCharacterStringType(t)
                        && !DataType.isBinaryStringType(type.getValueType())) {
                    throw DbException.getUnsupportedException("JSON_QUERY RETURNING " + type.getTraceSQL());
                }
            }
            break;
        case JSON_VALUE:
            if (onEmpty != null) {
                onEmpty = onEmpty.optimize(session);
            }
            if (onError != null) {
                onError = onError.optimize(session);
            }
            if (type == null) {
                type = TypeInfo.TYPE_VARCHAR;
            } else {
                int t = type.getValueType();
                if (!DataType.isCharacterStringType(t) && t != Value.BOOLEAN && !DataType.isNumericType(t)
                        && !DataType.isDateTimeType(t)) {
                    throw DbException.getUnsupportedException("JSON_VALUE RETURNING " + type.getTraceSQL());
                }
            }
        }
        if (right.isConstant()) {
            String pathString = right.getValue(session).getString();
            if (pathString == null) {
                return TypedValueExpression.get(ValueNull.INSTANCE, TypeInfo.TYPE_JSON);
            }
            path = JsonPath.get(pathString);
        }
        return this;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        super.mapColumns(resolver, level, state);
        if (passing != null) {
            for (Expression e : passing.values()) {
                e.mapColumns(resolver, level, state);
            }
        }
        if (function == JSON_VALUE) {
            if (onEmpty != null) {
                onEmpty.mapColumns(resolver, level, state);
            }
            if (onError != null) {
                onError.mapColumns(resolver, level, state);
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        super.setEvaluatable(tableFilter, value);
        if (passing != null) {
            for (Expression e : passing.values()) {
                e.setEvaluatable(tableFilter, value);
            }
        }
        if (function == JSON_VALUE) {
            if (onEmpty != null) {
                onEmpty.setEvaluatable(tableFilter, value);
            }
            if (onError != null) {
                onError.setEvaluatable(tableFilter, value);
            }
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        super.updateAggregate(session, stage);
        if (passing != null) {
            for (Expression e : passing.values()) {
                e.updateAggregate(session, stage);
            }
        }
        if (function == JSON_VALUE) {
            if (onEmpty != null) {
                onEmpty.updateAggregate(session, stage);
            }
            if (onError != null) {
                onError.updateAggregate(session, stage);
            }
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!super.isEverything(visitor)) {
            return false;
        }
        if (passing != null) {
            for (Expression e : passing.values()) {
                if (!e.isEverything(visitor)) {
                    return false;
                }
            }
        }
        if (function == JSON_VALUE) {
            if (onEmpty != null) {
                if (!onEmpty.isEverything(visitor)) {
                    return false;
                }
            }
            if (onError != null) {
                if (!onError.isEverything(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getUnenclosedSQL(builder.append(getName()).append('('), sqlFlags).append(", ");
        right.getUnenclosedSQL(builder, sqlFlags);
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
        if (function != JSON_EXISTS && type != null) {
            type.getSQL(builder.append(" RETURNING "), sqlFlags);
            if (function == JSON_QUERY && type.getValueType() != Value.JSON) {
                builder.append(" FORMAT JSON");
                if (encoding != null) {
                    builder.append(" ENCODING ").append(encoding.name());
                }
            }
        }
        switch (function) {
        case JSON_EXISTS:
            if (onError == ValueExpression.FALSE) {
                break;
            }
            builder.append(' ');
            if (onError == null) {
                builder.append("ERROR");
            } else {
                onError.getUnenclosedSQL(builder, sqlFlags);
            }
            builder.append(" ON ERROR");
            break;
        case JSON_QUERY:
            addJsonQuerySQL(builder, queryWrapperBehavior, queryQuotesBehavior, onEmpty, onError);
            break;
        case JSON_VALUE:
            addJsonValueOn(builder, sqlFlags, onEmpty, "ON EMPTY");
            addJsonValueOn(builder, sqlFlags, onError, "ON ERROR");
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return builder.append(')');
    }

    public static void addJsonQuerySQL(StringBuilder builder, QueryWrapperBehavior queryWrapperBehavior,
            QueryQuotesBehavior queryQuotesBehavior, Expression onEmpty, Expression onError) {
        if (queryWrapperBehavior != QueryWrapperBehavior.WITHOUT) {
            builder.append(' ').append(queryWrapperBehavior.getSQL());
        } else {
            if (queryQuotesBehavior == QueryQuotesBehavior.OMIT) {
                builder.append(" OMIT QUOTES");
            }
        }
        addJsonQueryOn(builder, onEmpty, "ON EMPTY");
        addJsonQueryOn(builder, onError, "ON ERROR");
    }

    private static void addJsonQueryOn(StringBuilder builder, Expression e, String clause) {
        if (e != ValueExpression.NULL) {
            builder.append(' ');
            if (e == null) {
                builder.append("ERROR");
            } else {
                Value v = e.getValue(null);
                if (v == ValueJson.EMPTY_ARRAY) {
                    builder.append("EMPTY ARRAY");
                } else if (v == ValueJson.EMPTY_OBJECT) {
                    builder.append("EMPTY OBJECT");
                } else {
                    throw DbException.getInternalError(v.getTraceSQL());
                }
            }
            builder.append(' ').append(clause);
        }
    }

    public static void addJsonValueOn(StringBuilder builder, int sqlFlags, Expression e, String clause) {
        if (e != ValueExpression.NULL) {
            builder.append(' ');
            if (e == null) {
                builder.append("ERROR");
            } else {
                e.getUnenclosedSQL(builder.append("DEFAULT "), sqlFlags);
            }
            builder.append(' ').append(clause);
        }
    }

    public static Map<String, JSONValue> convertParams(SessionLocal session, HashMap<String, Expression> passing) {
        Map<String, JSONValue> params;
        if (passing == null) {
            params = Map.of();
        } else {
            params = new HashMap<>();
            for (Entry<String, Expression> entry : passing.entrySet()) {
                Value p = entry.getValue().getValue(session);
                params.put(entry.getKey(), p == ValueNull.INSTANCE ? JSONNull.NULL
                        : DataType.isDateTimeType(p.getValueType()) ? new JSONDatetime(p)
                                : p.convertToJson(TypeInfo.TYPE_JSON, Value.CONVERT_TO, null).getDecomposition());
            }
        }
        return params;
    }

}
