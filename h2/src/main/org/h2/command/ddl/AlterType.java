/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * This class represents the statements ALTER TYPE
 */
public class AlterType extends AlterDomain {

    private String value;

    public AlterType(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public long update(Schema schema, Domain domain) {
        TypeInfo dataType = domain.getDataType();
        if (dataType.getValueType() != Value.ENUM) {
            throw DbException.get(ErrorCode.WRONG_OBJECT_TYPE, domainName);
        }

        ExtTypeInfoEnum oldExtTypeInfo = (ExtTypeInfoEnum) dataType.getExtTypeInfo();
        int count = oldExtTypeInfo.getCount();
        String[] newValues = new String[count + 1];
        for (int i = 0; i < count; i++) {
            newValues[i] = oldExtTypeInfo.getEnumerator(i);
        }
        newValues[count] = value;

        domain.setDataType(TypeInfo.getTypeInfo(Value.ENUM, -1L, -1, new ExtTypeInfoEnum(newValues)));
        schema.getDatabase().updateMeta(session, domain);

        forAllDependencies(session, domain, this::copyDomain, null, false);

        return 0;
    }

    private boolean copyDomain(Domain domain, Column column) {
        column.setType(domain.getDataType());
        return true;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TYPE;
    }
}
