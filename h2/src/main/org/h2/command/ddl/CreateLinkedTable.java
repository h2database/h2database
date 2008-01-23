/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.TableLink;

/**
 * This class represents the statement
 * CREATE LINKED TABLE
 */
public class CreateLinkedTable extends SchemaCommand {

    private String tableName;
    private String driver, url, user, password, originalTable;
    private boolean ifNotExists;
    private String comment;
    private boolean emitUpdates;
    private boolean force;

    public CreateLinkedTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setOriginalTable(String originalTable) {
        this.originalTable = originalTable;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        if (getSchema().findTableOrView(session, tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                    tableName);
        }
        int id = getObjectId(false, true);
        TableLink table = getSchema().createTableLink(id, tableName, driver, url, user, password, originalTable, emitUpdates, force);
        table.setComment(comment);
        db.addSchemaObject(session, table);
        return 0;
    }

    public void setEmitUpdates(boolean emitUpdates) {
        this.emitUpdates = emitUpdates;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

}
