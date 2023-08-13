/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSetMetaData;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.command.ParserBase;
import org.h2.command.ddl.SequenceOptions;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.util.HasSQL;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Typed;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueUuid;

/**
 * This class represents a column in a table.
 */
public final class Column implements HasSQL, Typed, ColumnTemplate {

    /**
     * The name of the rowid pseudo column.
     */
    public static final String ROWID = "_ROWID_";

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE =
            ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE =
            ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN =
            ResultSetMetaData.columnNullableUnknown;

    private TypeInfo type;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private Expression defaultExpression;
    private Expression onUpdateExpression;
    private SequenceOptions identityOptions;
    private boolean defaultOnNull;
    private Sequence sequence;
    private boolean isGeneratedAlways;
    private GeneratedColumnResolver generatedTableFilter;
    private int selectivity;
    private String comment;
    private boolean primaryKey;
    private boolean visible = true;
    private boolean rowId;
    private Domain domain;

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            columns
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, Column[] columns, int sqlFlags) {
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            columns[i].getSQL(builder, sqlFlags);
        }
        return builder;
    }

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            columns
     * @param separator
     *            separator
     * @param suffix
     *            additional SQL to append after each column
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, Column[] columns, String separator,
            String suffix, int sqlFlags) {
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                builder.append(separator);
            }
            columns[i].getSQL(builder, sqlFlags).append(suffix);
        }
        return builder;
    }

    public Column(String name, TypeInfo type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, TypeInfo type, Table table, int columnId) {
        this.name = name;
        this.type = type;
        this.table = table;
        this.columnId = columnId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null ||
                name == null || other.name == null) {
            return false;
        }
        if (table != other.table) {
            return false;
        }
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public Column getClone() {
        Column newColumn = new Column(name, type);
        newColumn.copy(this);
        return newColumn;
    }

    /**
     * Convert a value to this column's type without precision and scale checks.
     *
     * @param provider the cast information provider
     * @param v the value
     * @return the value
     */
    public Value convert(CastDataProvider provider, Value v) {
        try {
            return v.convertTo(type, provider, this);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                e = getDataConversionError(v, e);
            }
            throw e;
        }
    }

    /**
     * Returns whether this column is an identity column.
     *
     * @return whether this column is an identity column
     */
    public boolean isIdentity() {
        return sequence != null || identityOptions != null;
    }

    /**
     * Returns whether this column is a generated column.
     *
     * @return whether this column is a generated column
     */
    public boolean isGenerated() {
        return isGeneratedAlways && defaultExpression != null;
    }

    /**
     * Returns whether this column is a generated column or always generated
     * identity column.
     *
     * @return whether this column is a generated column or always generated
     *         identity column
     */
    public boolean isGeneratedAlways() {
        return isGeneratedAlways;
    }

    /**
     * Set the default value in the form of a generated expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setGeneratedExpression(Expression expression) {
        this.isGeneratedAlways = true;
        this.defaultExpression = expression;
    }

    /**
     * Set the table and column id.
     *
     * @param table the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public void setDefaultExpression(SessionLocal session, Expression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(
                        defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
        this.isGeneratedAlways = false;
    }

    @Override
    public void setOnUpdateExpression(SessionLocal session, Expression onUpdateExpression) {
        // also to test that no column names are used
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
            if (onUpdateExpression.isConstant()) {
                onUpdateExpression = ValueExpression.get(onUpdateExpression.getValue(session));
            }
        }
        this.onUpdateExpression = onUpdateExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    @Override
    public String getSQL(int sqlFlags) {
        return rowId ? name : ParserBase.quoteIdentifier(name, sqlFlags);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return rowId ? builder.append(name) : ParserUtil.quoteIdentifier(builder, name, sqlFlags);
    }

    /**
     * Appends the table name and column name to the specified builder.
     *
     * @param builder the string builder
     * @param sqlFlags formatting flags
     * @return the specified string builder
     */
    public StringBuilder getSQLWithTable(StringBuilder builder, int sqlFlags) {
        return getSQL(table.getSQL(builder, sqlFlags).append('.'), sqlFlags);
    }

    public String getName() {
        return name;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    public boolean getVisible() {
        return visible;
    }

    public void setVisible(boolean b) {
        visible = b;
    }

    @Override
    public Domain getDomain() {
        return domain;
    }

    @Override
    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    /**
     * Returns whether this column is a row identity column.
     *
     * @return true for _ROWID_ column, false otherwise
     */
    public boolean isRowId() {
        return rowId;
    }

    /**
     * Set row identity flag.
     *
     * @param rowId true _ROWID_ column, false otherwise
     */
    public void setRowId(boolean rowId) {
        this.rowId = rowId;
    }

    /**
     * Validate the value, convert it if required, and update the sequence value
     * if required. If the value is null, the default value (NULL if no default
     * is set) is returned. Domain constraints are validated as well.
     *
     * @param session the session
     * @param value the value or null
     * @param row the row
     * @return the new or converted value
     */
    Value validateConvertUpdateSequence(SessionLocal session, Value value, Row row) {
        check: {
            if (value == null) {
                if (sequence != null) {
                    value = session.getNextValueFor(sequence, null);
                    break check;
                }
                value = getDefaultOrGenerated(session, row);
            }
            if (value == ValueNull.INSTANCE && !nullable) {
                throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
            }
        }
        try {
            value = value.convertForAssignTo(type, session, name);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                e = getDataConversionError(value, e);
            }
            throw e;
        }
        if (domain != null) {
            domain.checkConstraints(session, value);
        }
        if (sequence != null && session.getMode().updateSequenceOnManualIdentityInsertion) {
            updateSequenceIfRequired(session, value.getLong());
        }
        return value;
    }

    private Value getDefaultOrGenerated(SessionLocal session, Row row) {
        Value value;
        Expression localDefaultExpression = getEffectiveDefaultExpression();
        if (localDefaultExpression == null) {
            value = ValueNull.INSTANCE;
        } else {
            if (isGeneratedAlways) {
                synchronized (this) {
                    generatedTableFilter.set(row);
                    try {
                        value = localDefaultExpression.getValue(session);
                    } finally {
                        generatedTableFilter.set(null);
                    }
                }
            } else {
                value = localDefaultExpression.getValue(session);
            }
        }
        return value;
    }

    private DbException getDataConversionError(Value value, DbException cause) {
        StringBuilder builder = new StringBuilder().append(value.getTraceSQL()).append(" (");
        if (table != null) {
            builder.append(table.getName()).append(": ");
        }
        builder.append(getCreateSQL()).append(')');
        return DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, cause, builder.toString());
    }

    private void updateSequenceIfRequired(SessionLocal session, long value) {
        if (sequence.getCycle() == Sequence.Cycle.EXHAUSTED) {
            return;
        }
        long current = sequence.getCurrentValue();
        long inc = sequence.getIncrement();
        if (inc > 0) {
            if (value < current) {
                return;
            }
        } else if (value > current) {
            return;
        }
        try {
            sequence.modify(value + inc, null, null, null, null, null, null);
        } catch (DbException ex) {
            if (ex.getErrorCode() == ErrorCode.SEQUENCE_ATTRIBUTES_INVALID_7) {
                return;
            }
            throw ex;
        }
        sequence.flush(session);
    }

    /**
     * Initialize the sequence for this column.
     *
     * @param session the session
     * @param schema the schema where the sequence should be generated
     * @param id the object id
     * @param temporary true if the sequence is temporary and does not need to
     *            be stored
     */
    public void initializeSequence(SessionLocal session, Schema schema, int id, boolean temporary) {
        if (identityOptions == null) {
            throw DbException.getInternalError();
        }
        String sequenceName;
        do {
            sequenceName = "SYSTEM_SEQUENCE_"
                    + StringUtils.toUpperEnglish(ValueUuid.getNewRandom().getString().replace('-', '_'));
        } while (schema.findSequence(sequenceName) != null);
        identityOptions.setDataType(type);
        Sequence seq = new Sequence(session, schema, id, sequenceName, identityOptions, true);
        seq.setTemporary(temporary);
        session.getDatabase().addSchemaObject(session, seq);
        // This method also ensures NOT NULL
        setSequence(seq, isGeneratedAlways);
    }

    @Override
    public void prepareExpressions(SessionLocal session) {
        if (defaultExpression != null) {
            if (isGeneratedAlways) {
                generatedTableFilter = new GeneratedColumnResolver(table);
                defaultExpression.mapColumns(generatedTableFilter, 0, Expression.MAP_INITIAL);
            }
            defaultExpression = defaultExpression.optimize(session);
        }
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
        }
        if (domain != null) {
            domain.prepareExpressions(session);
        }
    }

    public String getCreateSQLWithoutName() {
        return getCreateSQL(new StringBuilder(), false);
    }

    public String getCreateSQL() {
        return getCreateSQL(false);
    }

    /**
     * Get this columns part of CREATE TABLE SQL statement.
     *
     * @param forMeta whether this is for the metadata table
     * @return the SQL statement
     */
    public String getCreateSQL(boolean forMeta) {
        StringBuilder builder = new StringBuilder();
        if (name != null) {
            ParserUtil.quoteIdentifier(builder, name, DEFAULT_SQL_FLAGS).append(' ');
        }
        return getCreateSQL(builder, forMeta);
    }

    private String getCreateSQL(StringBuilder builder, boolean forMeta) {
        if (domain != null) {
            domain.getSQL(builder, DEFAULT_SQL_FLAGS);
        } else {
            type.getSQL(builder, DEFAULT_SQL_FLAGS);
        }
        if (!visible) {
            builder.append(" INVISIBLE ");
        }
        if (sequence != null) {
            builder.append(" GENERATED ").append(isGeneratedAlways ? "ALWAYS" : "BY DEFAULT").append(" AS IDENTITY");
            if (!forMeta) {
                sequence.getSequenceOptionsSQL(builder.append('(')).append(')');
            }
        } else if (defaultExpression != null) {
            if (isGeneratedAlways) {
                defaultExpression.getEnclosedSQL(builder.append(" GENERATED ALWAYS AS "), DEFAULT_SQL_FLAGS);
            } else {
                defaultExpression.getUnenclosedSQL(builder.append(" DEFAULT "), DEFAULT_SQL_FLAGS);
            }
        }
        if (onUpdateExpression != null) {
            onUpdateExpression.getUnenclosedSQL(builder.append(" ON UPDATE "), DEFAULT_SQL_FLAGS);
        }
        if (defaultOnNull) {
            builder.append(" DEFAULT ON NULL");
        }
        if (forMeta && sequence != null) {
            sequence.getSQL(builder.append(" SEQUENCE "), DEFAULT_SQL_FLAGS);
        }
        if (selectivity != 0) {
            builder.append(" SELECTIVITY ").append(selectivity);
        }
        if (comment != null) {
            StringUtils.quoteStringSQL(builder.append(" COMMENT "), comment);
        }
        if (!nullable) {
            builder.append(" NOT NULL");
        }
        return builder.toString();
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    @Override
    public Expression getEffectiveDefaultExpression() {
        /*
         * Identity columns may not have a default expression and may not use an
         * expression from domain.
         *
         * Generated columns always have an own expression.
         */
        if (sequence != null) {
            return null;
        }
        return defaultExpression != null ? defaultExpression
                : domain != null ? domain.getEffectiveDefaultExpression() : null;
    }

    @Override
    public Expression getOnUpdateExpression() {
        return onUpdateExpression;
    }

    @Override
    public Expression getEffectiveOnUpdateExpression() {
        /*
         * Identity and generated columns may not have an on update expression
         * and may not use an expression from domain.
         */
        if (sequence != null || isGeneratedAlways) {
            return null;
        }
        return onUpdateExpression != null ? onUpdateExpression
                : domain != null ? domain.getEffectiveOnUpdateExpression() : null;
    }

    /**
     * Whether the column has any identity options.
     *
     * @return true if yes
     */
    public boolean hasIdentityOptions() {
        return identityOptions != null;
    }

    /**
     * Set the identity options of this column.
     *
     * @param identityOptions
     *            identity column options
     * @param generatedAlways
     *            whether value should be always generated
     */
    public void setIdentityOptions(SequenceOptions identityOptions, boolean generatedAlways) {
        this.identityOptions = identityOptions;
        this.isGeneratedAlways = generatedAlways;
        removeNonIdentityProperties();
    }

    private void removeNonIdentityProperties() {
        nullable = false;
        onUpdateExpression = defaultExpression = null;
    }

    /**
     * Returns identity column options, or {@code null} if sequence was already
     * created or this column is not an identity column.
     *
     * @return identity column options, or {@code null}
     */
    public SequenceOptions getIdentityOptions() {
        return identityOptions;
    }

    public void setDefaultOnNull(boolean defaultOnNull) {
        this.defaultOnNull = defaultOnNull;
    }

    public boolean isDefaultOnNull() {
        return defaultOnNull;
    }

    /**
     * Rename the column. This method will only set the column name to the new
     * value.
     *
     * @param newName the new column name
     */
    public void rename(String newName) {
        this.name = newName;
    }

    /**
     * Set the sequence to generate the value.
     *
     * @param sequence the sequence
     * @param generatedAlways whether the value of the sequence is always used
     */
    public void setSequence(Sequence sequence, boolean generatedAlways) {
        this.sequence = sequence;
        this.isGeneratedAlways = generatedAlways;
        this.identityOptions = null;
        if (sequence != null) {
            removeNonIdentityProperties();
            if (sequence.getDatabase().getMode().identityColumnsHaveDefaultOnNull) {
                defaultOnNull = true;
            }
        }
    }

    public Sequence getSequence() {
        return sequence;
    }

    /**
     * Get the selectivity of the column. Selectivity 100 means values are
     * unique, 10 means every distinct value appears 10 times on average.
     *
     * @return the selectivity
     */
    public int getSelectivity() {
        return selectivity == 0 ? Constants.SELECTIVITY_DEFAULT : selectivity;
    }

    /**
     * Set the new selectivity of a column.
     *
     * @param selectivity the new value
     */
    public void setSelectivity(int selectivity) {
        selectivity = selectivity < 0 ? 0 : (selectivity > 100 ? 100 : selectivity);
        this.selectivity = selectivity;
    }

    @Override
    public String getDefaultSQL() {
        return defaultExpression == null ? null
                : defaultExpression.getUnenclosedSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getOnUpdateSQL() {
        return onUpdateExpression == null ? null
                : onUpdateExpression.getUnenclosedSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).toString();
    }

    public void setComment(String comment) {
        this.comment = comment != null && !comment.isEmpty() ? comment : null;
    }

    public String getComment() {
        return comment;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * Visit the default expression, the check constraint, and the sequence (if
     * any).
     *
     * @param visitor the visitor
     * @return true if every visited expression returned true, or if there are
     *         no expressions
     */
    boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.GET_DEPENDENCIES) {
            if (sequence != null) {
                visitor.getDependencies().add(sequence);
            }
        }
        Expression e = getEffectiveDefaultExpression();
        if (e != null && !e.isEverything(visitor)) {
            return false;
        }
        e = getEffectiveOnUpdateExpression();
        if (e != null && !e.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Check whether the new column is of the same type and not more restricted
     * than this column.
     *
     * @param newColumn the new (target) column
     * @return true if the new column is compatible
     */
    public boolean isWideningConversion(Column newColumn) {
        TypeInfo newType = newColumn.type;
        int valueType = type.getValueType();
        if (valueType != newType.getValueType()) {
            return false;
        }
        long precision = type.getPrecision();
        long newPrecision = newType.getPrecision();
        if (precision > newPrecision
                || precision < newPrecision && (valueType == Value.CHAR || valueType == Value.BINARY)) {
            return false;
        }
        if (type.getScale() != newType.getScale()) {
            return false;
        }
        if (!Objects.equals(type.getExtTypeInfo(), newType.getExtTypeInfo())) {
            return false;
        }
        if (nullable && !newColumn.nullable) {
            return false;
        }
        if (primaryKey != newColumn.primaryKey) {
            return false;
        }
        if (identityOptions != null || newColumn.identityOptions != null) {
            return false;
        }
        if (domain != newColumn.domain) {
            return false;
        }
        if (defaultExpression != null || newColumn.defaultExpression != null) {
            return false;
        }
        if (isGeneratedAlways || newColumn.isGeneratedAlways) {
            return false;
        }
        if (onUpdateExpression != null || newColumn.onUpdateExpression != null) {
            return false;
        }
        return true;
    }

    /**
     * Copy the data of the source column into the current column.
     *
     * @param source the source column
     */
    public void copy(Column source) {
        name = source.name;
        type = source.type;
        domain = source.domain;
        // table is not set
        // columnId is not set
        nullable = source.nullable;
        defaultExpression = source.defaultExpression;
        onUpdateExpression = source.onUpdateExpression;
        // identityOptions field is not set
        defaultOnNull = source.defaultOnNull;
        sequence = source.sequence;
        comment = source.comment;
        generatedTableFilter = source.generatedTableFilter;
        isGeneratedAlways = source.isGeneratedAlways;
        selectivity = source.selectivity;
        primaryKey = source.primaryKey;
        visible = source.visible;
    }

}
