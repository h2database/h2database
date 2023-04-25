/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Comment;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * COMMENT
 */
public class SetComment extends DefineCommand {

    private String schemaName;
    private String objectName;
    private boolean column;
    private String columnName;
    private int objectType;
    private Expression expr;

    public SetComment(SessionLocal session) {
        super(session);
    }

    @Override
    public long update() {
        Database db = getDatabase();
        DbObject object = null;
        int errorCode = ErrorCode.GENERAL_ERROR_1;
        if (schemaName == null) {
            schemaName = session.getCurrentSchemaName();
        }
        switch (objectType) {
        case DbObject.CONSTANT: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.getConstant(objectName);
            break;
        }
        case DbObject.CONSTRAINT: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.getConstraint(objectName);
            break;
        }
        case DbObject.FUNCTION_ALIAS: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.findFunction(objectName);
            errorCode = ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1;
            break;
        }
        case DbObject.INDEX: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.getIndex(objectName);
            break;
        }
        case DbObject.ROLE:
            session.getUser().checkAdmin();
            schemaName = null;
            object = db.findRole(objectName);
            errorCode = ErrorCode.ROLE_NOT_FOUND_1;
            break;
        case DbObject.SCHEMA: {
            schemaName = null;
            Schema schema = db.getSchema(objectName);
            session.getUser().checkSchemaOwner(schema);
            object = schema;
            break;
        }
        case DbObject.SEQUENCE: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.getSequence(objectName);
            break;
        }
        case DbObject.TABLE_OR_VIEW: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.getTableOrView(session, objectName);
            break;
        }
        case DbObject.TRIGGER: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.findTrigger(objectName);
            errorCode = ErrorCode.TRIGGER_NOT_FOUND_1;
            break;
        }
        case DbObject.USER:
            session.getUser().checkAdmin();
            schemaName = null;
            object = db.getUser(objectName);
            break;
        case DbObject.DOMAIN: {
            Schema schema = db.getSchema(schemaName);
            session.getUser().checkSchemaOwner(schema);
            object = schema.findDomain(objectName);
            errorCode = ErrorCode.DOMAIN_NOT_FOUND_1;
            break;
        }
        default:
        }
        if (object == null) {
            throw DbException.get(errorCode, objectName);
        }
        String text = expr.optimize(session).getValue(session).getString();
        if (text != null && text.isEmpty()) {
            text = null;
        }
        if (column) {
            Table table = (Table) object;
            table.getColumn(columnName).setComment(text);
        } else {
            object.setComment(text);
        }
        if (column || objectType == DbObject.TABLE_OR_VIEW ||
                objectType == DbObject.USER ||
                objectType == DbObject.INDEX ||
                objectType == DbObject.CONSTRAINT) {
            db.updateMeta(session, object);
        } else {
            Comment comment = db.findComment(object);
            if (comment == null) {
                if (text == null) {
                    // reset a non-existing comment - nothing to do
                } else {
                    int id = getObjectId();
                    comment = new Comment(db, id, object);
                    comment.setCommentText(text);
                    db.addDatabaseObject(session, comment);
                }
            } else {
                if (text == null) {
                    db.removeDatabaseObject(session, comment);
                } else {
                    comment.setCommentText(text);
                    db.updateMeta(session, comment);
                }
            }
        }
        return 0;
    }

    public void setCommentExpression(Expression expr) {
        this.expr = expr;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public void setObjectType(int objectType) {
        this.objectType = objectType;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setColumn(boolean column) {
        this.column = column;
    }

    @Override
    public int getType() {
        return CommandInterface.COMMENT;
    }

}
