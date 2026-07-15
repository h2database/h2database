/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER INDEX INVISIBLE / VISIBLE
 */
public class AlterIndexVisibility extends DefineCommand {

    private boolean ifExists;
    private Schema schema;
    private String indexName;
    private boolean invisible;

    public AlterIndexVisibility(SessionLocal session) {
        super(session);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setIndexName(String name) {
        indexName = name;
    }

    public void setInvisible(boolean invisible) {
        this.invisible = invisible;
    }

    @Override
    public long update() {
        Database db = getDatabase();
        Index index = schema.findIndex(session, indexName);
        if (index == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
            }
            return 0;
        }

        if (invisible && index.getIndexType().isPrimaryKey()) {
            throw DbException.getUnsupportedException("INVISIBLE on PRIMARY KEY");
        }

        session.getUser().checkTableRight(index.getTable(), Right.SCHEMA_OWNER);
        
        if (index.getIndexType().isInvisible() != invisible) {
            index.getIndexType().setInvisible(invisible);
            db.updateMeta(session, index);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_INDEX_VISIBILITY;
    }

}
