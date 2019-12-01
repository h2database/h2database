/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Domain;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP DOMAIN
 */
public class DropDomain extends SchemaCommand {

    private String typeName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropDomain(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = getSchema();
        Domain domain = schema.findDomain(typeName);
        if (domain == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, typeName);
            }
        } else {
            Column domainColumn = domain.getColumn();
            for (Table t : db.getAllTablesAndViews(false)) {
                boolean modified = false;
                for (Column c : t.getColumns()) {
                    Domain columnDomain = c.getDomain();
                    if (columnDomain != null && columnDomain.getName().equals(typeName)) {
                        if (dropAction == ConstraintActionType.RESTRICT) {
                            throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, t.getCreateSQL());
                        }
                        String columnName = c.getName();
                        Expression checkCondition = domainColumn.getCheckConstraint(session, columnName);
                        if (checkCondition != null) {
                            AlterTableAddConstraint check = new AlterTableAddConstraint(session, t.getSchema(), false);
                            check.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK);
                            check.setTableName(t.getName());
                            check.setCheckExpression(checkCondition);
                            check.update();
                        }
                        c.setOriginalSQL(domain.getColumn().getOriginalSQL());
                        Domain domain2 = domainColumn.getDomain();
                        c.setDomain(domain2);
                        c.removeCheckConstraint();
                        if (domain2 != null) {
                            c.addCheckConstraint(session, domain2.getColumn().getCheckConstraint(session, columnName));
                        }
                        modified = true;
                    }
                }
                if (modified) {
                    db.updateMeta(session, t);
                }
            }
            session.getDatabase().removeSchemaObject(session, domain);
        }
        return 0;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
