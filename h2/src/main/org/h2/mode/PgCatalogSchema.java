/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.schema.MetaSchema;
import org.h2.table.Table;

/**
 * {@code pg_catalog} schema.
 */
public final class PgCatalogSchema extends MetaSchema {

    private volatile HashMap<String, Table> tables;

    /**
     * Creates new instance of {@code pg_catalog} schema.
     *
     * @param database
     *            the database
     * @param owner
     *            the owner of the schema (system user)
     */
    public PgCatalogSchema(Database database, User owner) {
        super(database, Constants.PG_CATALOG_SCHEMA_ID, database.sysIdentifier(Constants.SCHEMA_PG_CATALOG), owner);
    }

    @Override
    protected Map<String, Table> getMap(SessionLocal session) {
        HashMap<String, Table> map = tables;
        if (map == null) {
            map = fillMap();
        }
        return map;
    }

    private synchronized HashMap<String, Table> fillMap() {
        HashMap<String, Table> map = tables;
        if (map == null) {
            map = database.newStringMap();
            for (int type = 0; type < PgCatalogTable.META_TABLE_TYPE_COUNT; type++) {
                PgCatalogTable table = new PgCatalogTable(this, Constants.PG_CATALOG_SCHEMA_ID - type, type);
                map.put(table.getName(), table);
            }
            tables = map;
        }
        return map;
    }

}
