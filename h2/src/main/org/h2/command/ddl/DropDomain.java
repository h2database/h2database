/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement DROP DOMAIN
 */
public class DropDomain extends SchemaCommand {

    private String typeName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropDomain(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintActionType.RESTRICT
                : ConstraintActionType.CASCADE;
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
        Schema schema = getSchema();
        Domain domain = schema.findDomain(typeName);
        if (domain == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, typeName);
            }
        } else {
            AlterDomain.forAllDependencies(session, domain, this::copyColumn, this::copyDomain, true);
            session.getDatabase().removeSchemaObject(session, domain);
        }
        return 0;
    }

    private boolean copyColumn(Domain domain, Column targetColumn) {
        Table targetTable = targetColumn.getTable();
        if (dropAction == ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, targetTable.getCreateSQL());
        }
        String columnName = targetColumn.getName();
        ArrayList<ConstraintDomain> constraints = domain.getConstraints();
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints) {
                Expression checkCondition = constraint.getCheckConstraint(session, columnName);
                AlterTableAddConstraint check = new AlterTableAddConstraint(session, targetTable.getSchema(),
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK, false);
                check.setTableName(targetTable.getName());
                check.setCheckExpression(checkCondition);
                check.update();
            }
        }
        copyExpressions(session, domain.getColumn(), targetColumn);
        return true;
    }

    private boolean copyDomain(Domain domain, Domain targetDomain) {
        if (dropAction == ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, targetDomain.getTraceSQL());
        }
        ArrayList<ConstraintDomain> constraints = domain.getConstraints();
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints) {
                Expression checkCondition = constraint.getCheckConstraint(session, null);
                AlterDomainAddConstraint check = new AlterDomainAddConstraint(session, targetDomain.getSchema(), //
                        false);
                check.setDomainName(targetDomain.getName());
                check.setCheckExpression(checkCondition);
                check.update();
            }
        }
        copyExpressions(session, domain.getColumn(), targetDomain.getColumn());
        return true;
    }

    private static boolean copyExpressions(Session session, Column domainColumn, Column targetColumn) {
        targetColumn.setDomain(domainColumn.getDomain());
        Expression e = domainColumn.getDefaultExpression();
        boolean modified = false;
        if (e != null && targetColumn.getDefaultExpression() == null) {
            targetColumn.setDefaultExpression(session, e);
            modified = true;
        }
        e = domainColumn.getOnUpdateExpression();
        if (e != null && targetColumn.getOnUpdateExpression() == null) {
            targetColumn.setOnUpdateExpression(session, e);
            modified = true;
        }
        return modified;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
