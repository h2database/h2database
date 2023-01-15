/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateSynonymData;
import org.h2.command.ddl.CreateTableData;
import org.h2.constraint.Constraint;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.DbSettings;
import org.h2.engine.Right;
import org.h2.engine.RightOwner;
import org.h2.engine.SessionLocal;
import org.h2.engine.SysProperties;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.MaterializedView;
import org.h2.table.MetaTable;
import org.h2.table.Table;
import org.h2.table.TableLink;
import org.h2.table.TableSynonym;
import org.h2.util.Utils;

/**
 * A schema as created by the SQL statement
 * CREATE SCHEMA
 */
public class Schema extends DbObject {

    private RightOwner owner;
    private final boolean system;
    private ArrayList<String> tableEngineParams;

    private final ConcurrentHashMap<String, Table> tablesAndViews;
    private final ConcurrentHashMap<String, Domain> domains;
    private final ConcurrentHashMap<String, TableSynonym> synonyms;
    private final ConcurrentHashMap<String, Index> indexes;
    private final ConcurrentHashMap<String, Sequence> sequences;
    private final ConcurrentHashMap<String, TriggerObject> triggers;
    private final ConcurrentHashMap<String, Constraint> constraints;
    private final ConcurrentHashMap<String, Constant> constants;
    private final ConcurrentHashMap<String, UserDefinedFunction> functionsAndAggregates;

    /**
     * The set of returned unique names that are not yet stored. It is used to
     * avoid returning the same unique name twice when multiple threads
     * concurrently create objects.
     */
    private final HashSet<String> temporaryUniqueNames = new HashSet<>();

    /**
     * Create a new schema object.
     *
     * @param database the database
     * @param id the object id
     * @param schemaName the schema name
     * @param owner the owner of the schema
     * @param system if this is a system schema (such a schema can not be
     *            dropped)
     */
    public Schema(Database database, int id, String schemaName, RightOwner owner, boolean system) {
        super(database, id, schemaName, Trace.SCHEMA);
        tablesAndViews = database.newConcurrentStringMap();
        domains = database.newConcurrentStringMap();
        synonyms = database.newConcurrentStringMap();
        indexes = database.newConcurrentStringMap();
        sequences = database.newConcurrentStringMap();
        triggers = database.newConcurrentStringMap();
        constraints = database.newConcurrentStringMap();
        constants = database.newConcurrentStringMap();
        functionsAndAggregates = database.newConcurrentStringMap();
        this.owner = owner;
        this.system = system;
    }

    /**
     * Check if this schema can be dropped. System schemas can not be dropped.
     *
     * @return true if it can be dropped
     */
    public boolean canDrop() {
        return !system;
    }

    @Override
    public String getCreateSQL() {
        if (system) {
            return null;
        }
        StringBuilder builder = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" AUTHORIZATION ");
        owner.getSQL(builder, DEFAULT_SQL_FLAGS);
        return builder.toString();
    }

    @Override
    public int getType() {
        return DbObject.SCHEMA;
    }

    /**
     * Return whether is this schema is empty (does not contain any objects).
     *
     * @return {@code true} if this schema is empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        return tablesAndViews.isEmpty() && domains.isEmpty() && synonyms.isEmpty() && indexes.isEmpty()
                && sequences.isEmpty() && triggers.isEmpty() && constraints.isEmpty() && constants.isEmpty()
                && functionsAndAggregates.isEmpty();
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = Utils.newSmallArrayList();
        ArrayList<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedObject() == this) {
                children.add(right);
            }
        }
        return children;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        removeChildrenFromMap(session, triggers);
        removeChildrenFromMap(session, constraints);
        // There can be dependencies between tables e.g. using computed columns,
        // so we might need to loop over them multiple times.
        boolean modified = true;
        while (!tablesAndViews.isEmpty()) {
            boolean newModified = false;
            for (Table obj : tablesAndViews.values()) {
                if (obj.getName() != null) {
                    // Database.removeSchemaObject() removes the object from
                    // the map too, but it is safe for ConcurrentHashMap.
                    Table dependentTable = database.getDependentTable(obj, obj);
                    if (dependentTable == null) {
                        database.removeSchemaObject(session, obj);
                        newModified = true;
                    } else if (dependentTable.getSchema() != this) {
                        throw DbException.get(ErrorCode.CANNOT_DROP_2, //
                                obj.getTraceSQL(), dependentTable.getTraceSQL());
                    } else if (!modified) {
                        dependentTable.removeColumnExpressionsDependencies(session);
                        dependentTable.setModified();
                        database.updateMeta(session, dependentTable);
                    }
                }
            }
            modified = newModified;
        }
        removeChildrenFromMap(session, domains);
        removeChildrenFromMap(session, indexes);
        removeChildrenFromMap(session, sequences);
        removeChildrenFromMap(session, constants);
        removeChildrenFromMap(session, functionsAndAggregates);
        for (Right right : database.getAllRights()) {
            if (right.getGrantedObject() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        owner = null;
        invalidate();
    }

    private void removeChildrenFromMap(SessionLocal session, ConcurrentHashMap<String, ? extends SchemaObject> map) {
        if (!map.isEmpty()) {
            for (SchemaObject obj : map.values()) {
                /*
                 * Referential constraints are dropped when unique or PK
                 * constraint is dropped, but iterator may return already
                 * removed objects in some cases.
                 */
                if (obj.isValid()) {
                    // Database.removeSchemaObject() removes the object from
                    // the map too, but it is safe for ConcurrentHashMap.
                    database.removeSchemaObject(session, obj);
                }
            }
        }
    }

    /**
     * Get the owner of this schema.
     *
     * @return the owner
     */
    public RightOwner getOwner() {
        return owner;
    }

    /**
     * Get table engine params of this schema.
     *
     * @return default table engine params
     */
    public ArrayList<String> getTableEngineParams() {
        return tableEngineParams;
    }

    /**
     * Set table engine params of this schema.
     * @param tableEngineParams default table engine params
     */
    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        this.tableEngineParams = tableEngineParams;
    }

    @SuppressWarnings("unchecked")
    private Map<String, SchemaObject> getMap(int type) {
        Map<String, ? extends SchemaObject> result;
        switch (type) {
        case DbObject.TABLE_OR_VIEW:
            result = tablesAndViews;
            break;
        case DbObject.DOMAIN:
            result = domains;
            break;
        case DbObject.SYNONYM:
            result = synonyms;
            break;
        case DbObject.SEQUENCE:
            result = sequences;
            break;
        case DbObject.INDEX:
            result = indexes;
            break;
        case DbObject.TRIGGER:
            result = triggers;
            break;
        case DbObject.CONSTRAINT:
            result = constraints;
            break;
        case DbObject.CONSTANT:
            result = constants;
            break;
        case DbObject.FUNCTION_ALIAS:
        case DbObject.AGGREGATE:
            result = functionsAndAggregates;
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return (Map<String, SchemaObject>) result;
    }

    /**
     * Add an object to this schema.
     * This method must not be called within CreateSchemaObject;
     * use Database.addSchemaObject() instead
     *
     * @param obj the object to add
     */
    public void add(SchemaObject obj) {
        if (obj.getSchema() != this) {
            throw DbException.getInternalError("wrong schema");
        }
        String name = obj.getName();
        Map<String, SchemaObject> map = getMap(obj.getType());
        if (map.putIfAbsent(name, obj) != null) {
            throw DbException.getInternalError("object already exists: " + name);
        }
        freeUniqueName(name);
    }

    /**
     * Rename an object.
     *
     * @param obj the object to rename
     * @param newName the new name
     */
    public void rename(SchemaObject obj, String newName) {
        int type = obj.getType();
        Map<String, SchemaObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName()) && !(obj instanceof MetaTable)) {
                throw DbException.getInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                throw DbException.getInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        map.remove(obj.getName());
        freeUniqueName(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
        freeUniqueName(newName);
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also
     * returned. Synonyms are not returned or resolved.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Table findTableOrView(SessionLocal session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        return table;
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also
     * returned. If a synonym with this name exists, the backing table of the
     * synonym is returned
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Table resolveTableOrView(SessionLocal session, String name) {
        return resolveTableOrView(session, name, /*resolveMaterializedView*/true);
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also
     * returned. If a synonym with this name exists, the backing table of the
     * synonym is returned
     *
     * @param session the session
     * @param name the object name
     * @param resolveMaterializedView if true, and the object is a materialized
     *             view, return the underlying Table object.
     * @return the object or null
     */
    public Table resolveTableOrView(SessionLocal session, String name, boolean resolveMaterializedView) {
        Table table = findTableOrView(session, name);
        if (table == null) {
            TableSynonym synonym = synonyms.get(name);
            if (synonym != null) {
                return synonym.getSynonymFor();
            }
        }
        if (resolveMaterializedView && table instanceof MaterializedView) {
            MaterializedView matView = (MaterializedView) table;
            return matView.getUnderlyingTable();
        }
        return table;
    }

    /**
     * Try to find a synonym with this name. This method returns null if
     * no object with this name exists.
     *
     * @param name the object name
     * @return the object or null
     */
    public TableSynonym getSynonym(String name) {
        return synonyms.get(name);
    }

    /**
     * Get the domain if it exists, or null if not.
     *
     * @param name the name of the domain
     * @return the domain or null
     */
    public Domain findDomain(String name) {
        return domains.get(name);
    }

    /**
     * Try to find an index with this name. This method returns null if
     * no object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Index findIndex(SessionLocal session, String name) {
        Index index = indexes.get(name);
        if (index == null) {
            index = session.findLocalTempTableIndex(name);
        }
        return index;
    }

    /**
     * Try to find a trigger with this name. This method returns null if
     * no object with this name exists.
     *
     * @param name the object name
     * @return the object or null
     */
    public TriggerObject findTrigger(String name) {
        return triggers.get(name);
    }

    /**
     * Try to find a sequence with this name. This method returns null if
     * no object with this name exists.
     *
     * @param sequenceName the object name
     * @return the object or null
     */
    public Sequence findSequence(String sequenceName) {
        return sequences.get(sequenceName);
    }

    /**
     * Try to find a constraint with this name. This method returns null if no
     * object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Constraint findConstraint(SessionLocal session, String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null) {
            constraint = session.findLocalTempTableConstraint(name);
        }
        return constraint;
    }

    /**
     * Try to find a user defined constant with this name. This method returns
     * null if no object with this name exists.
     *
     * @param constantName the object name
     * @return the object or null
     */
    public Constant findConstant(String constantName) {
        return constants.get(constantName);
    }

    /**
     * Try to find a user defined function with this name. This method returns
     * null if no object with this name exists.
     *
     * @param functionAlias the object name
     * @return the object or null
     */
    public FunctionAlias findFunction(String functionAlias) {
        UserDefinedFunction userDefinedFunction = findFunctionOrAggregate(functionAlias);
        return userDefinedFunction instanceof FunctionAlias ? (FunctionAlias) userDefinedFunction : null;
    }

    /**
     * Get the user defined aggregate function if it exists. This method returns
     * null if no object with this name exists.
     *
     * @param name the name of the user defined aggregate function
     * @return the aggregate function or null
     */
    public UserAggregate findAggregate(String name) {
        UserDefinedFunction userDefinedFunction = findFunctionOrAggregate(name);
        return userDefinedFunction instanceof UserAggregate ? (UserAggregate) userDefinedFunction : null;
    }

    /**
     * Try to find a user defined function or aggregate function with the
     * specified name. This method returns null if no object with this name
     * exists.
     *
     * @param name
     *            the object name
     * @return the object or null
     */
    public UserDefinedFunction findFunctionOrAggregate(String name) {
        return functionsAndAggregates.get(name);
    }

    /**
     * Reserve a unique object name.
     *
     * @param name the object name
     */
    public void reserveUniqueName(String name) {
        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.add(name);
            }
        }
    }

    /**
     * Release a unique object name.
     *
     * @param name the object name
     */
    public void freeUniqueName(String name) {
        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.remove(name);
            }
        }
    }

    private String getUniqueName(DbObject obj, Map<String, ? extends SchemaObject> map, String prefix) {
        StringBuilder nameBuilder = new StringBuilder(prefix);
        String hash = Integer.toHexString(obj.getName().hashCode());
        synchronized (temporaryUniqueNames) {
            for (int i = 0, len = hash.length(); i < len; i++) {
                char c = hash.charAt(i);
                String name = nameBuilder.append(c >= 'a' ? (char) (c - 0x20) : c).toString();
                if (!map.containsKey(name) && temporaryUniqueNames.add(name)) {
                    return name;
                }
            }
            int nameLength = nameBuilder.append('_').length();
            for (int i = 0;; i++) {
                String name = nameBuilder.append(i).toString();
                if (!map.containsKey(name) && temporaryUniqueNames.add(name)) {
                    return name;
                }
                nameBuilder.setLength(nameLength);
            }
        }
    }

    /**
     * Create a unique constraint name.
     *
     * @param session the session
     * @param table the constraint table
     * @return the unique name
     */
    public String getUniqueConstraintName(SessionLocal session, Table table) {
        Map<String, Constraint> tableConstraints;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableConstraints = session.getLocalTempTableConstraints();
        } else {
            tableConstraints = constraints;
        }
        return getUniqueName(table, tableConstraints, "CONSTRAINT_");
    }

    /**
     * Create a unique constraint name.
     *
     * @param session the session
     * @param domain the constraint domain
     * @return the unique name
     */
    public String getUniqueDomainConstraintName(SessionLocal session, Domain domain) {
        return getUniqueName(domain, constraints, "CONSTRAINT_");
    }

    /**
     * Create a unique index name.
     *
     * @param session the session
     * @param table the indexed table
     * @param prefix the index name prefix
     * @return the unique name
     */
    public String getUniqueIndexName(SessionLocal session, Table table, String prefix) {
        Map<String, Index> tableIndexes;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableIndexes = session.getLocalTempTableIndexes();
        } else {
            tableIndexes = indexes;
        }
        return getUniqueName(table, tableIndexes, prefix);
    }

    /**
     * Get the table or view with the given name.
     * Local temporary tables are also returned.
     *
     * @param session the session
     * @param name the table or view name
     * @return the table or view
     * @throws DbException if no such object exists
     */
    public Table getTableOrView(SessionLocal session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null) {
            if (session != null) {
                table = session.findLocalTempTable(name);
            }
            if (table == null) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, name);
            }
        }
        return table;
    }

    /**
     * Get the domain with the given name.
     *
     * @param name the domain name
     * @return the domain
     * @throws DbException if no such object exists
     */
    public Domain getDomain(String name) {
        Domain domain = domains.get(name);
        if (domain == null) {
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, name);
        }
        return domain;
    }

    /**
     * Get the index with the given name.
     *
     * @param name the index name
     * @return the index
     * @throws DbException if no such object exists
     */
    public Index getIndex(String name) {
        Index index = indexes.get(name);
        if (index == null) {
            throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, name);
        }
        return index;
    }

    /**
     * Get the constraint with the given name.
     *
     * @param name the constraint name
     * @return the constraint
     * @throws DbException if no such object exists
     */
    public Constraint getConstraint(String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, name);
        }
        return constraint;
    }

    /**
     * Get the user defined constant with the given name.
     *
     * @param constantName the constant name
     * @return the constant
     * @throws DbException if no such object exists
     */
    public Constant getConstant(String constantName) {
        Constant constant = constants.get(constantName);
        if (constant == null) {
            throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
        }
        return constant;
    }

    /**
     * Get the sequence with the given name.
     *
     * @param sequenceName the sequence name
     * @return the sequence
     * @throws DbException if no such object exists
     */
    public Sequence getSequence(String sequenceName) {
        Sequence sequence = sequences.get(sequenceName);
        if (sequence == null) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
        }
        return sequence;
    }

    /**
     * Get all objects.
     *
     * @param addTo
     *                  list to add objects to, or {@code null} to allocate a new
     *                  list
     * @return the specified list with added objects, or a new (possibly empty) list
     *         with all objects
     */
    public ArrayList<SchemaObject> getAll(ArrayList<SchemaObject> addTo) {
        if (addTo == null) {
            addTo = Utils.newSmallArrayList();
        }
        addTo.addAll(tablesAndViews.values());
        addTo.addAll(domains.values());
        addTo.addAll(synonyms.values());
        addTo.addAll(sequences.values());
        addTo.addAll(indexes.values());
        addTo.addAll(triggers.values());
        addTo.addAll(constraints.values());
        addTo.addAll(constants.values());
        addTo.addAll(functionsAndAggregates.values());
        return addTo;
    }

    /**
     * Get all objects of the given type.
     *
     * @param type
     *                  the object type
     * @param addTo
     *                  list to add objects to
     */
    public void getAll(int type, ArrayList<SchemaObject> addTo) {
        addTo.addAll(getMap(type).values());
    }

    public Collection<Domain> getAllDomains() {
        return domains.values();
    }

    public Collection<Constraint> getAllConstraints() {
        return constraints.values();
    }

    public Collection<Constant> getAllConstants() {
        return constants.values();
    }

    public Collection<Sequence> getAllSequences() {
        return sequences.values();
    }

    public Collection<TriggerObject> getAllTriggers() {
        return triggers.values();
    }

    /**
     * Get all tables and views.
     *
     * @param session the session, {@code null} to exclude meta tables
     * @return a (possible empty) list of all objects
     */
    public Collection<Table> getAllTablesAndViews(SessionLocal session) {
        return tablesAndViews.values();
    }

    public Collection<Index> getAllIndexes() {
        return indexes.values();
    }

    public Collection<TableSynonym> getAllSynonyms() {
        return synonyms.values();
    }

    public Collection<UserDefinedFunction> getAllFunctionsAndAggregates() {
        return functionsAndAggregates.values();
    }

    /**
     * Get the table with the given name, if any.
     *
     * @param session the session
     * @param name the table name
     * @return the table or null if not found
     */
    public Table getTableOrViewByName(SessionLocal session, String name) {
        return tablesAndViews.get(name);
    }

    /**
     * Remove an object from this schema.
     *
     * @param obj the object to remove
     */
    public void remove(SchemaObject obj) {
        String objName = obj.getName();
        Map<String, SchemaObject> map = getMap(obj.getType());
        if (map.remove(objName) == null) {
            throw DbException.getInternalError("not found: " + objName);
        }
        freeUniqueName(objName);
    }

    /**
     * Add a table to the schema.
     *
     * @param data the create table information
     * @return the created {@link Table} object
     */
    public Table createTable(CreateTableData data) {
        synchronized (database) {
            if (!data.temporary || data.globalTemporary) {
                database.lockMeta(data.session);
            }
            data.schema = this;
            String tableEngine = data.tableEngine;
            if (tableEngine == null) {
                DbSettings s = database.getSettings();
                tableEngine = s.defaultTableEngine;
                if (tableEngine == null) {
                    return database.getStore().createTable(data);
                }
                data.tableEngine = tableEngine;
            }
            if (data.tableEngineParams == null) {
                data.tableEngineParams = this.tableEngineParams;
            }
            return database.getTableEngine(tableEngine).createTable(data);
        }
    }

    /**
     * Add a table synonym to the schema.
     *
     * @param data the create synonym information
     * @return the created {@link TableSynonym} object
     */
    public TableSynonym createSynonym(CreateSynonymData data) {
        synchronized (database) {
            database.lockMeta(data.session);
            data.schema = this;
            return new TableSynonym(data);
        }
    }

    /**
     * Add a linked table to the schema.
     *
     * @param id the object id
     * @param tableName the table name of the alias
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param originalSchema the schema name of the target table
     * @param originalTable the table name of the target table
     * @param emitUpdates if updates should be emitted instead of delete/insert
     * @param force create the object even if the database can not be accessed
     * @return the {@link TableLink} object
     */
    public TableLink createTableLink(int id, String tableName, String driver,
            String url, String user, String password, String originalSchema,
            String originalTable, boolean emitUpdates, boolean force) {
        synchronized (database) {
            return new TableLink(this, id, tableName,
                    driver, url, user, password,
                    originalSchema, originalTable, emitUpdates, force);
        }
    }

}
