package org.h2.expression.function;

import org.h2.command.dml.MergeUsing;
import org.h2.engine.SessionLocal;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueVarchar;

public class MergeAction extends Operation0 {
    private final MergeUsing statement;

    public MergeAction(final MergeUsing statement) {
        this.statement = statement;
    }

    @Override
    public String getAlias(SessionLocal session, int columnIndex) {
        return "MERGE_ACTION";
    }

    @Override
    public Value getValue(final SessionLocal session) {
        return ValueVarchar.get(statement.getCurrentAction().name(), session);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.getTypeInfo(Value.VARCHAR);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append("MERGE_ACTION()");
    }

    @Override
    public boolean isEverything(final ExpressionVisitor visitor) {
        return false;
    }

    @Override
    public int getCost() {
        return 3;
    }
}
