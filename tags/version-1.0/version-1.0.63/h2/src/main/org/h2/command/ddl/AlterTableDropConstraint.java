/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constraint.Constraint;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.schema.Schema;

/**
 * @author Thomas
 */
public class AlterTableDropConstraint extends SchemaCommand {

    private String constraintName;

    public AlterTableDropConstraint(Session session, Schema schema) {
        super(session, schema);
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    public int update() throws SQLException {
        session.commit(true);
        Constraint constraint = getSchema().getConstraint(constraintName);
        session.getUser().checkRight(constraint.getTable(), Right.ALL);
        session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
        session.getDatabase().removeSchemaObject(session, constraint);
        return 0;
    }

}
