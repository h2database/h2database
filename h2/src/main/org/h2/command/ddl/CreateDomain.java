/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.HasSQL;
import org.h2.util.Utils;
import org.h2.value.DataType;

/**
 * This class represents the statement
 * CREATE DOMAIN
 */
public class CreateDomain extends SchemaCommand {

    private String typeName;
    private Column column;
    private boolean ifNotExists;

    private ArrayList<AlterDomainAddConstraint> constraintCommands;

    public CreateDomain(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        session.getUser().checkAdmin();
        Schema schema = getSchema();
        if (schema.findDomain(typeName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, typeName);
        }
        DataType builtIn = DataType.getTypeByName(typeName, session.getDatabase().getMode());
        if (builtIn != null) {
            if (!builtIn.hidden) {
                throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, typeName);
            }
            Table table = session.getDatabase().getFirstUserTable();
            if (table != null) {
                StringBuilder builder = new StringBuilder(typeName).append(" (");
                table.getSQL(builder, HasSQL.TRACE_SQL_FLAGS).append(')');
                throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, builder.toString());
            }
        }
        int id = getObjectId();
        Domain domain = new Domain(schema, id, typeName);
        domain.setColumn(column);
        schema.getDatabase().addSchemaObject(session, domain);
        if (constraintCommands != null) {
            for (AlterDomainAddConstraint command : constraintCommands) {
                command.update();
            }
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_DOMAIN;
    }

    public void addConstraintCommand(AlterDomainAddConstraint command) {
        if (constraintCommands == null) {
            constraintCommands = Utils.newSmallArrayList();
        }
        constraintCommands.add(command);
    }

}
