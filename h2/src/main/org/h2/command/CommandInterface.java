/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import org.h2.expression.ParameterInterface;
import org.h2.result.BatchResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;

/**
 * Represents a SQL statement.
 */
public interface CommandInterface extends AutoCloseable {

    /**
     * The type for unknown statement.
     */
    int UNKNOWN = 0;

    // ddl operations

    /**
     * The type of ALTER INDEX RENAME statement.
     */
    int ALTER_INDEX_RENAME = 1;

    /**
     * The type of ALTER SCHEMA RENAME statement.
     */
    int ALTER_SCHEMA_RENAME = 2;

    /**
     * The type of ALTER TABLE ADD CHECK statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_CHECK = 3;

    /**
     * The type of ALTER TABLE ADD UNIQUE statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_UNIQUE = 4;

    /**
     * The type of ALTER TABLE ADD FOREIGN KEY statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL = 5;

    /**
     * The type of ALTER TABLE ADD PRIMARY KEY statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY = 6;

    /**
     * The type of ALTER TABLE ADD statement.
     */
    int ALTER_TABLE_ADD_COLUMN = 7;

    /**
     * The type of ALTER TABLE ALTER COLUMN SET NOT NULL statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_NOT_NULL = 8;

    /**
     * The type of ALTER TABLE ALTER COLUMN DROP NOT NULL statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL = 9;

    /**
     * The type of ALTER TABLE ALTER COLUMN SET DEFAULT and ALTER TABLE ALTER
     * COLUMN DROP DEFAULT statements.
     */
    int ALTER_TABLE_ALTER_COLUMN_DEFAULT = 10;

    /**
     * The type of ALTER TABLE ALTER COLUMN statement that changes the column
     * data type.
     */
    int ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE = 11;

    /**
     * The type of ALTER TABLE DROP COLUMN statement.
     */
    int ALTER_TABLE_DROP_COLUMN = 12;

    /**
     * The type of ALTER TABLE ALTER COLUMN SELECTIVITY statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_SELECTIVITY = 13;

    /**
     * The type of ALTER TABLE DROP CONSTRAINT statement.
     */
    int ALTER_TABLE_DROP_CONSTRAINT = 14;

    /**
     * The type of ALTER TABLE RENAME statement.
     */
    int ALTER_TABLE_RENAME = 15;

    /**
     * The type of ALTER TABLE ALTER COLUMN RENAME statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_RENAME = 16;

    /**
     * The type of ALTER USER ADMIN statement.
     */
    int ALTER_USER_ADMIN = 17;

    /**
     * The type of ALTER USER RENAME statement.
     */
    int ALTER_USER_RENAME = 18;

    /**
     * The type of ALTER USER SET PASSWORD statement.
     */
    int ALTER_USER_SET_PASSWORD = 19;

    /**
     * The type of ALTER VIEW statement.
     */
    int ALTER_VIEW = 20;

    /**
     * The type of ANALYZE statement.
     */
    int ANALYZE = 21;

    /**
     * The type of CREATE AGGREGATE statement.
     */
    int CREATE_AGGREGATE = 22;

    /**
     * The type of CREATE CONSTANT statement.
     */
    int CREATE_CONSTANT = 23;

    /**
     * The type of CREATE ALIAS statement.
     */
    int CREATE_ALIAS = 24;

    /**
     * The type of CREATE INDEX statement.
     */
    int CREATE_INDEX = 25;

    /**
     * The type of CREATE LINKED TABLE statement.
     */
    int CREATE_LINKED_TABLE = 26;

    /**
     * The type of CREATE ROLE statement.
     */
    int CREATE_ROLE = 27;

    /**
     * The type of CREATE SCHEMA statement.
     */
    int CREATE_SCHEMA = 28;

    /**
     * The type of CREATE SEQUENCE statement.
     */
    int CREATE_SEQUENCE = 29;

    /**
     * The type of CREATE TABLE statement.
     */
    int CREATE_TABLE = 30;

    /**
     * The type of CREATE TRIGGER statement.
     */
    int CREATE_TRIGGER = 31;

    /**
     * The type of CREATE USER statement.
     */
    int CREATE_USER = 32;

    /**
     * The type of CREATE DOMAIN statement.
     */
    int CREATE_DOMAIN = 33;

    /**
     * The type of CREATE VIEW statement.
     */
    int CREATE_VIEW = 34;

    /**
     * The type of DEALLOCATE statement.
     */
    int DEALLOCATE = 35;

    /**
     * The type of DROP AGGREGATE statement.
     */
    int DROP_AGGREGATE = 36;

    /**
     * The type of DROP CONSTANT statement.
     */
    int DROP_CONSTANT = 37;

    /**
     * The type of DROP ALL OBJECTS statement.
     */
    int DROP_ALL_OBJECTS = 38;

    /**
     * The type of DROP ALIAS statement.
     */
    int DROP_ALIAS = 39;

    /**
     * The type of DROP INDEX statement.
     */
    int DROP_INDEX = 40;

    /**
     * The type of DROP ROLE statement.
     */
    int DROP_ROLE = 41;

    /**
     * The type of DROP SCHEMA statement.
     */
    int DROP_SCHEMA = 42;

    /**
     * The type of DROP SEQUENCE statement.
     */
    int DROP_SEQUENCE = 43;

    /**
     * The type of DROP TABLE statement.
     */
    int DROP_TABLE = 44;

    /**
     * The type of DROP TRIGGER statement.
     */
    int DROP_TRIGGER = 45;

    /**
     * The type of DROP USER statement.
     */
    int DROP_USER = 46;

    /**
     * The type of DROP DOMAIN statement.
     */
    int DROP_DOMAIN = 47;

    /**
     * The type of DROP VIEW statement.
     */
    int DROP_VIEW = 48;

    /**
     * The type of GRANT statement.
     */
    int GRANT = 49;

    /**
     * The type of REVOKE statement.
     */
    int REVOKE = 50;

    /**
     * The type of PREPARE statement.
     */
    int PREPARE = 51;

    /**
     * The type of COMMENT statement.
     */
    int COMMENT = 52;

    /**
     * The type of TRUNCATE TABLE statement.
     */
    int TRUNCATE_TABLE = 53;

    // dml operations

    /**
     * The type of ALTER SEQUENCE statement.
     */
    int ALTER_SEQUENCE = 54;

    /**
     * The type of ALTER TABLE SET REFERENTIAL_INTEGRITY statement.
     */
    int ALTER_TABLE_SET_REFERENTIAL_INTEGRITY = 55;

    /**
     * The type of BACKUP statement.
     */
    int BACKUP = 56;

    /**
     * The type of CALL statement.
     */
    int CALL = 57;

    /**
     * The type of DELETE statement.
     */
    int DELETE = 58;

    /**
     * The type of EXECUTE statement.
     */
    int EXECUTE = 59;

    /**
     * The type of EXPLAIN statement.
     */
    int EXPLAIN = 60;

    /**
     * The type of INSERT statement.
     */
    int INSERT = 61;

    /**
     * The type of MERGE statement.
     */
    int MERGE = 62;

    /**
     * The type of REPLACE statement.
     */
    int REPLACE = 63;

    /**
     * The type of no operation statement.
     */
    int NO_OPERATION = 63;

    /**
     * The type of RUNSCRIPT statement.
     */
    int RUNSCRIPT = 64;

    /**
     * The type of SCRIPT statement.
     */
    int SCRIPT = 65;

    /**
     * The type of SELECT statement.
     */
    int SELECT = 66;

    /**
     * The type of SET statement.
     */
    int SET = 67;

    /**
     * The type of UPDATE statement.
     */
    int UPDATE = 68;

    // transaction commands

    /**
     * The type of SET AUTOCOMMIT statement.
     */
    int SET_AUTOCOMMIT_TRUE = 69;

    /**
     * The type of SET AUTOCOMMIT statement.
     */
    int SET_AUTOCOMMIT_FALSE = 70;

    /**
     * The type of COMMIT statement.
     */
    int COMMIT = 71;

    /**
     * The type of ROLLBACK statement.
     */
    int ROLLBACK = 72;

    /**
     * The type of CHECKPOINT statement.
     */
    int CHECKPOINT = 73;

    /**
     * The type of SAVEPOINT statement.
     */
    int SAVEPOINT = 74;

    /**
     * The type of ROLLBACK TO SAVEPOINT statement.
     */
    int ROLLBACK_TO_SAVEPOINT = 75;

    /**
     * The type of CHECKPOINT SYNC statement.
     */
    int CHECKPOINT_SYNC = 76;

    /**
     * The type of PREPARE COMMIT statement.
     */
    int PREPARE_COMMIT = 77;

    /**
     * The type of COMMIT TRANSACTION statement.
     */
    int COMMIT_TRANSACTION = 78;

    /**
     * The type of ROLLBACK TRANSACTION statement.
     */
    int ROLLBACK_TRANSACTION = 79;

    /**
     * The type of SHUTDOWN statement.
     */
    int SHUTDOWN = 80;

    /**
     * The type of SHUTDOWN IMMEDIATELY statement.
     */
    int SHUTDOWN_IMMEDIATELY = 81;

    /**
     * The type of SHUTDOWN COMPACT statement.
     */
    int SHUTDOWN_COMPACT = 82;

    /**
     * The type of BEGIN {WORK|TRANSACTION} statement.
     */
    int BEGIN = 83;

    /**
     * The type of SHUTDOWN DEFRAG statement.
     */
    int SHUTDOWN_DEFRAG = 84;

    /**
     * The type of ALTER TABLE RENAME CONSTRAINT statement.
     */
    int ALTER_TABLE_RENAME_CONSTRAINT = 85;

    /**
     * The type of EXPLAIN ANALYZE statement.
     */
    int EXPLAIN_ANALYZE = 86;

    /**
     * The type of ALTER TABLE ALTER COLUMN SET INVISIBLE statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_VISIBILITY = 87;

    /**
     * The type of CREATE SYNONYM statement.
     */
    int CREATE_SYNONYM = 88;

    /**
     * The type of DROP SYNONYM statement.
     */
    int DROP_SYNONYM = 89;

    /**
     * The type of ALTER TABLE ALTER COLUMN SET ON UPDATE statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_ON_UPDATE = 90;

    /**
     * The type of EXECUTE IMMEDIATELY statement.
     */
    int EXECUTE_IMMEDIATELY = 91;

    /**
     * The type of ALTER DOMAIN ADD CONSTRAINT statement.
     */
    int ALTER_DOMAIN_ADD_CONSTRAINT = 92;

    /**
     * The type of ALTER DOMAIN DROP CONSTRAINT statement.
     */
    int ALTER_DOMAIN_DROP_CONSTRAINT = 93;

    /**
     * The type of ALTER DOMAIN SET DEFAULT and ALTER DOMAIN DROP DEFAULT
     * statements.
     */
    int ALTER_DOMAIN_DEFAULT = 94;

    /**
     * The type of ALTER DOMAIN SET ON UPDATE and ALTER DOMAIN DROP ON UPDATE
     * statements.
     */
    int ALTER_DOMAIN_ON_UPDATE = 95;

    /**
     * The type of ALTER DOMAIN RENAME statement.
     */
    int ALTER_DOMAIN_RENAME = 96;

    /**
     * The type of HELP statement.
     */
    int HELP = 97;

    /**
     * The type of ALTER TABLE ALTER COLUMN DROP EXPRESSION statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_DROP_EXPRESSION = 98;

    /**
     * The type of ALTER TABLE ALTER COLUMN DROP IDENTITY statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_DROP_IDENTITY = 99;

    /**
     * The type of ALTER TABLE ALTER COLUMN SET DEFAULT ON NULL and ALTER TABLE
     * ALTER COLUMN DROP DEFAULT ON NULL statements.
     */
    int ALTER_TABLE_ALTER_COLUMN_DEFAULT_ON_NULL = 100;

    /**
     * The type of ALTER DOMAIN RENAME CONSTRAINT statement.
     */
    int ALTER_DOMAIN_RENAME_CONSTRAINT = 101;

    /**
     * The type of CREATE MATERIALIZED VIEW statement.
     */
    int CREATE_MATERIALIZED_VIEW = 102;

    /**
     * The type of REFRESH MATERIALIZED VIEW statement.
     */
    int REFRESH_MATERIALIZED_VIEW = 103;

    /**
     * The type of DROP MATERIALIZED VIEW statement.
     */
    int DROP_MATERIALIZED_VIEW = 104;

    /**
     * The type of ALTER TYPE statement.
     */
    int ALTER_TYPE = 105;

    /**
     * Get command type.
     *
     * @return one of the constants above
     */
    int getCommandType();

    /**
     * Check if this is a query.
     *
     * @return true if it is a query
     */
    boolean isQuery();

    /**
     * Get the parameters (if any).
     *
     * @return the parameters
     */
    ArrayList<? extends ParameterInterface> getParameters();

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param fetchSize the number of rows to fetch (for remote commands only)
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    ResultInterface executeQuery(long maxRows, int fetchSize, boolean scrollable);

    /**
     * Execute the statement
     *
     * @param generatedKeysRequest
     *            {@code null} or {@code false} if generated keys are not
     *            needed, {@code true} if generated keys should be configured
     *            automatically, {@code int[]} to specify column indices to
     *            return generated keys from, or {@code String[]} to specify
     *            column names to return generated keys from
     *
     * @return the update count and generated keys, if any
     */
    ResultWithGeneratedKeys executeUpdate(Object generatedKeysRequest);


    /**
     * Executes the statement with multiple sets of parameters.
     *
     * @param batchParameters
     *            batch parameters
     * @param generatedKeysRequest
     *            {@code null} or {@code false} if generated keys are not needed,
     *            {@code true} if generated keys should be configured
     *            automatically, {@code int[]} to specify column indices to
     *            return generated keys from, or {@code String[]} to specify
     *            column names to return generated keys from
     * @return result of batch execution
     */
    BatchResult executeBatchUpdate(ArrayList<Value[]> batchParameters, Object generatedKeysRequest);

    /**
     * Stop the command execution, release all locks and resources.
     *
     * @param commitIfAutoCommit
     *            commit the session if auto-commit is enabled
     */
    void stop(boolean commitIfAutoCommit);

    /**
     * Close the statement.
     */
    @Override
    void close();

    /**
     * Cancel the statement if it is still processing.
     */
    void cancel();

    /**
     * Get an empty result set containing the meta data of the result.
     *
     * @return the empty result
     */
    ResultInterface getMetaData();

}
