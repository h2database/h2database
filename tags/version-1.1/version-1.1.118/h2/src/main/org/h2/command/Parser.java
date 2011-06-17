/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.Collator;
import java.util.HashSet;

import org.h2.api.Trigger;
import org.h2.command.ddl.AlterIndexRename;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.AlterTableDropConstraint;
import org.h2.command.ddl.AlterTableRename;
import org.h2.command.ddl.AlterTableRenameColumn;
import org.h2.command.ddl.AlterUser;
import org.h2.command.ddl.AlterView;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CreateAggregate;
import org.h2.command.ddl.CreateConstant;
import org.h2.command.ddl.CreateFunctionAlias;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateLinkedTable;
import org.h2.command.ddl.CreateRole;
import org.h2.command.ddl.CreateSchema;
import org.h2.command.ddl.CreateSequence;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.CreateTrigger;
import org.h2.command.ddl.CreateUser;
import org.h2.command.ddl.CreateUserDataType;
import org.h2.command.ddl.CreateView;
import org.h2.command.ddl.DeallocateProcedure;
import org.h2.command.ddl.DropAggregate;
import org.h2.command.ddl.DropConstant;
import org.h2.command.ddl.DropDatabase;
import org.h2.command.ddl.DropFunctionAlias;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropRole;
import org.h2.command.ddl.DropSchema;
import org.h2.command.ddl.DropSequence;
import org.h2.command.ddl.DropTable;
import org.h2.command.ddl.DropTrigger;
import org.h2.command.ddl.DropUser;
import org.h2.command.ddl.DropUserDataType;
import org.h2.command.ddl.DropView;
import org.h2.command.ddl.GrantRevoke;
import org.h2.command.ddl.PrepareProcedure;
import org.h2.command.ddl.SetComment;
import org.h2.command.ddl.TruncateTable;
import org.h2.command.dml.AlterSequence;
import org.h2.command.dml.AlterTableSet;
import org.h2.command.dml.BackupCommand;
import org.h2.command.dml.Call;
import org.h2.command.dml.Delete;
import org.h2.command.dml.ExecuteProcedure;
import org.h2.command.dml.ExplainPlan;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.NoOperation;
import org.h2.command.dml.Query;
import org.h2.command.dml.RunScriptCommand;
import org.h2.command.dml.ScriptCommand;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.command.dml.SelectUnion;
import org.h2.command.dml.Set;
import org.h2.command.dml.SetTypes;
import org.h2.command.dml.TransactionCommand;
import org.h2.command.dml.Update;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Procedure;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.Aggregate;
import org.h2.expression.Alias;
import org.h2.expression.CompareLike;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.ConditionExists;
import org.h2.expression.ConditionIn;
import org.h2.expression.ConditionInSelect;
import org.h2.expression.ConditionNot;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.Function;
import org.h2.expression.FunctionCall;
import org.h2.expression.JavaAggregate;
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.Rownum;
import org.h2.expression.SequenceValue;
import org.h2.expression.Subquery;
import org.h2.expression.TableFunction;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.expression.Wildcard;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.util.ByteUtils;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.util.StringCache;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * The parser is used to convert a SQL statement string to an command object.
 */
public class Parser {

    // used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8, CHAR_DOLLAR_QUOTED_STRING = 9;

    // this are token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, PARAMETER = 3, END = 4, VALUE = 5;
    private static final int EQUAL = 6, BIGGER_EQUAL = 7, BIGGER = 8;
    private static final int SMALLER = 9, SMALLER_EQUAL = 10, NOT_EQUAL = 11, AT = 12;
    private static final int MINUS = 17, PLUS = 18;
    private static final int STRING_CONCAT = 22;
    private static final int OPEN = 31, CLOSE = 32, NULL = 34, TRUE = 40, FALSE = 41;

    private static final int CURRENT_TIMESTAMP = 42, CURRENT_DATE = 43, CURRENT_TIME = 44, ROWNUM = 45;

    private final Database database;
    private final Session session;

    private int[] characterTypes;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private Value currentValue;
    private String sqlCommand;
    private String originalSQL;
    private char[] sqlCommandChars;
    private int lastParseIndex;
    private int parseIndex;
    private Prepared prepared;
    private Prepared currentPrepared;
    private Select currentSelect;
    private ObjectArray<Parameter> parameters;
    private String schemaName;
    private ObjectArray<String> expectedList;
    private boolean rightsChecked;
    private boolean recompileAlways;
    private ObjectArray<Parameter> indexedParameterList;

    public Parser(Session session) {
        database = session.getDatabase();
        this.session = session;
    }

    /**
     * Parse the statement and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    public Prepared prepare(String sql) throws SQLException {
        try {
            Prepared p = parse(sql);
            p.prepare();
            return p;
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * Parse the statement, but don't prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    public Prepared parseOnly(String sql) throws SQLException {
        try {
            return parse(sql);
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * Parse a statement or a list of statements, and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the command object
     */
    public Command prepareCommand(String sql) throws SQLException {
        try {
            Prepared p = parse(sql);
            p.prepare();
            Command c = new CommandContainer(this, sql, p);
            p.setCommand(c);
            if (isToken(";")) {
                String remaining = originalSQL.substring(parseIndex);
                if (remaining.trim().length() != 0) {
                    CommandList list = new CommandList(this, sql, c, remaining);
                    // list.addCommand(c);
                    // do {
                    // c = parseCommand();
                    // list.addCommand(c);
                    // } while(currentToken.equals(";"));
                    c = list;
                }
            } else if (currentTokenType != END) {
                throw getSyntaxError();
            }
            return c;
        } catch (Exception e) {
            throw Message.addSQL(Message.convert(e), this.originalSQL);
        }
    }

    private Prepared parse(String sql) throws SQLException {
        Prepared p;
        try {
            // first, try the fast variant
            p = parse(sql, false);
        } catch (SQLException e) {
            if (e.getErrorCode() == ErrorCode.SYNTAX_ERROR_1) {
                // now, get the detailed exception
                p = parse(sql, true);
            } else {
                throw Message.addSQL(e, sql);
            }
        }
        p.setPrepareAlways(recompileAlways);
        p.setParameterList(parameters);
        return p;
    }

    private Prepared parse(String sql, boolean withExpectedList) throws SQLException {
        initialize(sql);
        if (withExpectedList) {
            expectedList = ObjectArray.newInstance();
        } else {
            expectedList = null;
        }
        parameters = ObjectArray.newInstance();
        currentSelect = null;
        currentPrepared = null;
        prepared = null;
        recompileAlways = false;
        indexedParameterList = null;
        read();
        return parsePrepared();
    }

    private Prepared parsePrepared() throws SQLException {
        int start = lastParseIndex;
        Prepared c = null;
        String token = currentToken;
        if (token.length() == 0) {
            c = new NoOperation(session);
        } else {
            char first = token.charAt(0);
            switch (first) {
            case '(':
                c = parseSelect();
                break;
            case 'A':
                if (readIf("ALTER")) {
                    c = parseAlter();
                } else if (readIf("ANALYZE")) {
                    c = parseAnalyze();
                }
                break;
            case 'B':
                if (readIf("BACKUP")) {
                    c = parseBackup();
                } else if (readIf("BEGIN")) {
                    c = parseBegin();
                }
                break;
            case 'C':
                if (readIf("COMMIT")) {
                    c = parseCommit();
                } else if (readIf("CREATE")) {
                    c = parseCreate();
                } else if (readIf("CALL")) {
                    c = parserCall();
                } else if (readIf("CHECKPOINT")) {
                    c = parseCheckpoint();
                } else if (readIf("COMMENT")) {
                    c = parseComment();
                }
                break;
            case 'D':
                if (readIf("DELETE")) {
                    c = parseDelete();
                } else if (readIf("DROP")) {
                    c = parseDrop();
                } else if (readIf("DECLARE")) {
                    // support for DECLARE GLOBAL TEMPORARY TABLE...
                    c = parseCreate();
                } else if (readIf("DEALLOCATE")) {
                    c = parseDeallocate();
                }
                break;
            case 'E':
                if (readIf("EXPLAIN")) {
                    c = parseExplain();
                } else if (readIf("EXECUTE")) {
                    c = parseExecute();
                }
                break;
            case 'F':
                if (isToken("FROM")) {
                    c = parseSelect();
                }
                break;
            case 'G':
                if (readIf("GRANT")) {
                    c = parseGrantRevoke(GrantRevoke.GRANT);
                }
                break;
            case 'H':
                if (readIf("HELP")) {
                    c = parseHelp();
                }
                break;
            case 'I':
                if (readIf("INSERT")) {
                    c = parseInsert();
                }
                break;
            case 'M':
                if (readIf("MERGE")) {
                    c = parseMerge();
                }
                break;
            case 'P':
                if (readIf("PREPARE")) {
                    c = parsePrepare();
                }
                break;
            case 'R':
                if (readIf("ROLLBACK")) {
                    c = parseRollback();
                } else if (readIf("REVOKE")) {
                    c = parseGrantRevoke(GrantRevoke.REVOKE);
                } else if (readIf("RUNSCRIPT")) {
                    c = parseRunScript();
                } else if (readIf("RELEASE")) {
                    c = parseReleaseSavepoint();
                }
                break;
            case 'S':
                if (isToken("SELECT")) {
                    c = parseSelect();
                } else if (readIf("SET")) {
                    c = parseSet();
                } else if (readIf("SAVEPOINT")) {
                    c = parseSavepoint();
                } else if (readIf("SCRIPT")) {
                    c = parseScript();
                } else if (readIf("SHUTDOWN")) {
                    c = parseShutdown();
                } else if (readIf("SHOW")) {
                    c = parseShow();
                }
                break;
            case 'T':
                if (readIf("TRUNCATE")) {
                    c = parseTruncate();
                }
                break;
            case 'U':
                if (readIf("UPDATE")) {
                    c = parseUpdate();
                }
                break;
            case 'V':
                if (readIf("VALUES")) {
                    c = parserCall();
                }
                break;
            case 'W':
                if (readIf("WITH")) {
                    c = parserWith();
                }
                break;
            default:
                throw getSyntaxError();
            }
            if (indexedParameterList != null) {
                for (int i = 0; i < indexedParameterList.size(); i++) {
                    if (indexedParameterList.get(i) == null) {
                        indexedParameterList.set(i, new Parameter(i));
                    }
                }
                parameters = indexedParameterList;
            }
            if (readIf("{")) {
                do {
                    int index = (int) readLong() - 1;
                    if (index < 0 || index >= parameters.size()) {
                        throw getSyntaxError();
                    }
                    Parameter p = parameters.get(index);
                    if (p == null) {
                        throw getSyntaxError();
                    }
                    read(":");
                    Expression expr = readExpression();
                    expr = expr.optimize(session);
                    p.setValue(expr.getValue(session));
                } while (readIf(","));
                read("}");
                for (Parameter p : parameters) {
                    p.checkSet();
                }
                parameters.clear();
            }
        }
        if (c == null) {
            throw getSyntaxError();
        }
        setSQL(c, null, start);
        return c;
    }

    private SQLException getSyntaxError() {
        if (expectedList == null || expectedList.size() == 0) {
            return Message.getSyntaxError(sqlCommand, parseIndex);
        }
        StatementBuilder buff = new StatementBuilder();
        for (String e : expectedList) {
            buff.appendExceptFirst(", ");
            buff.append(e);
        }
        return Message.getSyntaxError(sqlCommand, parseIndex, buff.toString());
    }

    private Prepared parseBackup() throws SQLException {
        BackupCommand command = new BackupCommand(session);
        read("TO");
        command.setFileName(readExpression());
        return command;
    }

    private Prepared parseAnalyze() throws SQLException {
        Analyze command = new Analyze(session);
        if (readIf("SAMPLE_SIZE")) {
            command.setTop(getPositiveInt());
        }
        return command;
    }

    private TransactionCommand parseBegin() throws SQLException {
        TransactionCommand command;
        if (!readIf("WORK")) {
            readIf("TRANSACTION");
        }
        command = new TransactionCommand(session, TransactionCommand.BEGIN);
        return command;
    }

    private TransactionCommand parseCommit() throws SQLException {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, TransactionCommand.COMMIT_TRANSACTION);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        command = new TransactionCommand(session, TransactionCommand.COMMIT);
        readIf("WORK");
        return command;
    }

    private TransactionCommand parseShutdown() throws SQLException {
        int type = TransactionCommand.SHUTDOWN;
        if (readIf("IMMEDIATELY")) {
            type = TransactionCommand.SHUTDOWN_IMMEDIATELY;
        } else {
            if (!readIf("COMPACT")) {
                readIf("SCRIPT");
            }
        }
        return new TransactionCommand(session, type);
    }

    private TransactionCommand parseRollback() throws SQLException {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TRANSACTION);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        if (readIf("TO")) {
            read("SAVEPOINT");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TO_SAVEPOINT);
            command.setSavepointName(readUniqueIdentifier());
        } else {
            readIf("WORK");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK);
        }
        return command;
    }

    private Prepared parsePrepare() throws SQLException {
        if (readIf("COMMIT")) {
            TransactionCommand command = new TransactionCommand(session, TransactionCommand.PREPARE_COMMIT);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        String procedureName = readAliasIdentifier();
        if (readIf("(")) {
            ObjectArray<Column> list = ObjectArray.newInstance();
            for (int i = 0;; i++) {
                Column column = parseColumnForTable("C" + i);
                list.add(column);
                if (readIf(")")) {
                    break;
                }
                read(",");
            }
        }
        read("AS");
        Prepared prep = parsePrepared();
        PrepareProcedure command = new PrepareProcedure(session);
        command.setProcedureName(procedureName);
        command.setPrepared(prep);
        return command;
    }

    private TransactionCommand parseSavepoint() throws SQLException {
        TransactionCommand command = new TransactionCommand(session, TransactionCommand.SAVEPOINT);
        command.setSavepointName(readUniqueIdentifier());
        return command;
    }

    private Prepared parseReleaseSavepoint() throws SQLException {
        Prepared command = new NoOperation(session);
        readIf("SAVEPOINT");
        readUniqueIdentifier();
        return command;
    }

    private Schema getSchema() throws SQLException {
        if (schemaName == null) {
            return null;
        }
        Schema schema = database.findSchema(schemaName);
        if (schema == null) {
            if ("SESSION".equals(schemaName)) {
                // for local temporary tables
                schema = database.getSchema(session.getCurrentSchemaName());
            } else {
                throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
            }
        }
        return schema;
    }

    private Column readTableColumn(TableFilter filter) throws SQLException {
        String tableAlias = null;
        String columnName = readColumnIdentifier();
        if (readIf(".")) {
            tableAlias = columnName;
            columnName = readColumnIdentifier();
            if (readIf(".")) {
                String schema = tableAlias;
                tableAlias = columnName;
                columnName = readColumnIdentifier();
                if (readIf(".")) {
                    String catalogName = schema;
                    schema = tableAlias;
                    tableAlias = columnName;
                    columnName = readColumnIdentifier();
                    if (!catalogName.equals(database.getShortName())) {
                        throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, catalogName);
                    }
                }
                if (!schema.equals(filter.getTable().getSchema().getName())) {
                    throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schema);
                }
            }
            if (!tableAlias.equals(filter.getTableAlias())) {
                throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
            }
        }
        return filter.getTable().getColumn(columnName);
    }

    private Update parseUpdate() throws SQLException {
        Update command = new Update(session);
        currentPrepared = command;
        int start = lastParseIndex;
        TableFilter filter = readSimpleTableFilter();
        command.setTableFilter(filter);
        read("SET");
        if (readIf("(")) {
            ObjectArray<Column> columns = ObjectArray.newInstance();
            do {
                Column column = readTableColumn(filter);
                columns.add(column);
            } while (readIf(","));
            read(")");
            read("=");
            Expression expression = readExpression();
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                Function f = Function.getFunction(database, "ARRAY_GET");
                f.setParameter(0, expression);
                f.setParameter(1, ValueExpression.get(ValueInt.get(i + 1)));
                f.doneWithParameters();
                command.setAssignment(column, f);
            }
        } else {
            do {
                Column column = readTableColumn(filter);
                read("=");
                Expression expression;
                if (readIf("DEFAULT")) {
                    expression = ValueExpression.getDefault();
                } else {
                    expression = readExpression();
                }
                command.setAssignment(column, expression);
            } while (readIf(","));
        }
        if (readIf("WHERE")) {
            Expression condition = readExpression();
            command.setCondition(condition);
        }
        setSQL(command, "UPDATE", start);
        return command;
    }

    private TableFilter readSimpleTableFilter() throws SQLException {
        Table table = readTableOrView();
        String alias = null;
        if (readIf("AS")) {
            alias = readAliasIdentifier();
        } else if (currentTokenType == IDENTIFIER) {
            if (!"SET".equals(currentToken)) {
                // SET is not a keyword (PostgreSQL supports it as a table name)
                alias = readAliasIdentifier();
            }
        }
        return new TableFilter(session, table, alias, rightsChecked, currentSelect);
    }

    private Delete parseDelete() throws SQLException {
        Delete command = new Delete(session);
        currentPrepared = command;
        int start = lastParseIndex;
        readIf("FROM");
        TableFilter filter = readSimpleTableFilter();
        command.setTableFilter(filter);
        if (readIf("WHERE")) {
            Expression condition = readExpression();
            command.setCondition(condition);
        }
        setSQL(command, "DELETE", start);
        return command;
    }

    private IndexColumn[] parseIndexColumnList() throws SQLException {
        ObjectArray<IndexColumn> columns = ObjectArray.newInstance();
        do {
            IndexColumn column = new IndexColumn();
            column.columnName = readColumnIdentifier();
            columns.add(column);
            if (readIf("ASC")) {
                // ignore
            } else if (readIf("DESC")) {
                column.sortType = SortOrder.DESCENDING;
            }
            if (readIf("NULLS")) {
                if (readIf("FIRST")) {
                    column.sortType |= SortOrder.NULLS_FIRST;
                } else {
                    read("LAST");
                    column.sortType |= SortOrder.NULLS_LAST;
                }
            }
        } while (readIf(","));
        read(")");
        IndexColumn[] cols = new IndexColumn[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private String[] parseColumnList() throws SQLException {
        ObjectArray<String> columns = ObjectArray.newInstance();
        do {
            String columnName = readColumnIdentifier();
            columns.add(columnName);
        } while (readIfMore());
        String[] cols = new String[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private Column[] parseColumnList(Table table) throws SQLException {
        ObjectArray<Column> columns = ObjectArray.newInstance();
        HashSet<Column> set = New.hashSet();
        if (!readIf(")")) {
            do {
                Column column = table.getColumn(readColumnIdentifier());
                if (!set.add(column)) {
                    throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getSQL());
                }
                columns.add(column);
            } while (readIfMore());
        }
        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private boolean readIfMore() throws SQLException {
        if (readIf(",")) {
            return !readIf(")");
        }
        read(")");
        return false;
    }

    private Prepared parseHelp() throws SQLException {
        StringBuilder buff = new StringBuilder("SELECT * FROM INFORMATION_SCHEMA.HELP");
        int i = 0;
        ObjectArray<Value> paramValues = ObjectArray.newInstance();
        while (currentTokenType != END) {
            String s = currentToken;
            read();
            if (i == 0) {
                buff.append(" WHERE ");
            } else {
                buff.append(" AND ");
            }
            i++;
            buff.append("UPPER(TOPIC) LIKE ?");
            paramValues.add(ValueString.get("%" + s + "%"));
        }
        return prepare(session, buff.toString(), paramValues);
    }

    private Prepared parseShow() throws SQLException {
        ObjectArray<Value> paramValues = ObjectArray.newInstance();
        StringBuilder buff = new StringBuilder("SELECT ");
        if (readIf("CLIENT_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UNICODE' AS CLIENT_ENCODING FROM DUAL");
        } else if (readIf("DATESTYLE")) {
            // for PostgreSQL compatibility
            buff.append("'ISO' AS DATESTYLE FROM DUAL");
        } else if (readIf("SERVER_VERSION")) {
            // for PostgreSQL compatibility
            buff.append("'8.1.4' AS SERVER_VERSION FROM DUAL");
        } else if (readIf("SERVER_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UTF8' AS SERVER_ENCODING FROM DUAL");
        } else if (readIf("TABLES")) {
            // for MySQL compatibility
            String schema = Constants.SCHEMA_MAIN;
            if (readIf("FROM")) {
                schema = readUniqueIdentifier();
            }
            buff.append("TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME");
            paramValues.add(ValueString.get(schema));
        } else if (readIf("COLUMNS")) {
            // for MySQL compatibility
            read("FROM");
            String tableName = readUniqueIdentifier();
            paramValues.add(ValueString.get(tableName));
            String schema = Constants.SCHEMA_MAIN;
            if (readIf("FROM")) {
                schema = readUniqueIdentifier();
            }
            buff.append("C.COLUMN_NAME FIELD, " +
                    "C.TYPE_NAME || '(' || C.NUMERIC_PRECISION || ')' TYPE, " +
                    "C.IS_NULLABLE \"NULL\", " +
                    "CASE I.INDEX_TYPE_NAME WHEN 'PRIMARY KEY' THEN 'PRI' WHEN 'UNIQUE INDEX' THEN 'UNI' ELSE '' END KEY, " +
                    "IFNULL(COLUMN_DEFAULT, 'NULL') DEFAULT " +
                    "FROM INFORMATION_SCHEMA.COLUMNS C LEFT OUTER JOIN INFORMATION_SCHEMA.INDEXES I " +
                    "ON I.TABLE_SCHEMA=C.TABLE_SCHEMA " +
                    "AND I.TABLE_NAME=C.TABLE_NAME " +
                    "AND I.COLUMN_NAME=C.COLUMN_NAME " +
                    "WHERE C.TABLE_NAME=? AND C.TABLE_SCHEMA=? " +
                    "ORDER BY C.ORDINAL_POSITION");
            paramValues.add(ValueString.get(schema));
        } else if (readIf("DATABASES") || readIf("SCHEMAS")) {
            // for MySQL compatibility
            buff.append("SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        }
        return prepare(session, buff.toString(), paramValues);
    }

    private Prepared prepare(Session s, String sql, ObjectArray<Value> paramValues) throws SQLException {
        Prepared prep = s.prepare(sql);
        ObjectArray<Parameter> params = prep.getParameters();
        for (int i = 0; params != null && i < params.size(); i++) {
            Parameter p = params.get(i);
            p.setValue(paramValues.get(i));
        }
        return prep;
    }

    private Merge parseMerge() throws SQLException {
        Merge command = new Merge(session);
        currentPrepared = command;
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        if (readIf("(")) {
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf("KEY")) {
            read("(");
            Column[] keys = parseColumnList(table);
            command.setKeys(keys);
        }
        if (readIf("VALUES")) {
            do {
                ObjectArray<Expression> values = ObjectArray.newInstance();
                read("(");
                if (!readIf(")")) {
                    do {
                        if (readIf("DEFAULT")) {
                            values.add(null);
                        } else {
                            values.add(readExpression());
                        }
                    } while (readIfMore());
                }
                Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            } while (readIf(","));
        } else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private Insert parseInsert() throws SQLException {
        Insert command = new Insert(session);
        currentPrepared = command;
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        if (readIf("(")) {
            if (isToken("SELECT") || isToken("FROM")) {
                command.setQuery(parseSelect());
                read(")");
                return command;
            }
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf("DEFAULT")) {
            read("VALUES");
            Expression[] expr = new Expression[0];
            command.addRow(expr);
        } else if (readIf("VALUES")) {
            do {
                ObjectArray<Expression> values = ObjectArray.newInstance();
                read("(");
                if (!readIf(")")) {
                    do {
                        if (readIf("DEFAULT")) {
                            values.add(null);
                        } else {
                            values.add(readExpression());
                        }
                    } while (readIfMore());
                }
                Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            } while (readIf(","));
        } else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private TableFilter readTableFilter(boolean fromOuter) throws SQLException {
        Table table;
        String alias = null;
        if (readIf("(")) {
            if (isToken("SELECT") || isToken("FROM")) {
                int start = lastParseIndex;
                int paramIndex = parameters.size();
                Query query = parseSelectUnion();
                read(")");
                query = parseSelectUnionExtension(query, start, true);
                ObjectArray<Parameter> params = ObjectArray.newInstance();
                for (int i = paramIndex; i < parameters.size(); i++) {
                    params.add(parameters.get(i));
                }
                query.setParameterList(params);
                query.init();
                Session s;
                if (prepared != null && prepared instanceof CreateView) {
                    s = database.getSystemSession();
                } else {
                    s = session;
                }
                alias = session.getNextSystemIdentifier(sqlCommand);
                table = TableView.createTempView(s, session.getUser(), alias, query, currentSelect);
            } else {
                TableFilter top = readTableFilter(fromOuter);
                top = readJoin(top, currentSelect, fromOuter);
                read(")");
                alias = readFromAlias(null);
                if (alias != null) {
                    top.setAlias(alias);
                }
                return top;
            }
        } else {
            String tableName = readIdentifierWithSchema(null);
            if (readIf("(")) {
                Schema mainSchema = database.getSchema(Constants.SCHEMA_MAIN);
                if (tableName.equals(RangeTable.NAME)) {
                    Expression min = readExpression();
                    read(",");
                    Expression max = readExpression();
                    read(")");
                    table = new RangeTable(mainSchema, min, max);
                } else {
                    Expression func = readFunction(tableName);
                    if (!(func instanceof FunctionCall)) {
                        throw getSyntaxError();
                    }
                    table = new FunctionTable(mainSchema, session, func, (FunctionCall) func);
                }
            } else if ("DUAL".equals(tableName)) {
                table = getDualTable();
            } else {
                table = readTableOrView(tableName);
            }
        }
        alias = readFromAlias(alias);
        return new TableFilter(session, table, alias, rightsChecked, currentSelect);
    }

    private String readFromAlias(String alias) throws SQLException {
        if (readIf("AS")) {
            alias = readAliasIdentifier();
        } else if (currentTokenType == IDENTIFIER) {
            // left and right are not keywords (because they are functions as
            // well)
            if (!isToken("LEFT") && !isToken("RIGHT") && !isToken("FULL")) {
                alias = readAliasIdentifier();
            }
        }
        return alias;
    }

    private Prepared parseTruncate() throws SQLException {
        read("TABLE");
        Table table = readTableOrView();
        TruncateTable command = new TruncateTable(session);
        command.setTable(table);
        return command;
    }

    private boolean readIfExists(boolean ifExists) throws SQLException {
        if (readIf("IF")) {
            read("EXISTS");
            ifExists = true;
        }
        return ifExists;
    }

    private Prepared parseComment() throws SQLException {
        int type = 0;
        read("ON");
        boolean column = false;
        if (readIf("TABLE") || readIf("VIEW")) {
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("COLUMN")) {
            column = true;
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("CONSTANT")) {
            type = DbObject.CONSTANT;
        } else if (readIf("CONSTRAINT")) {
            type = DbObject.CONSTRAINT;
        } else if (readIf("ALIAS")) {
            type = DbObject.FUNCTION_ALIAS;
        } else if (readIf("INDEX")) {
            type = DbObject.INDEX;
        } else if (readIf("ROLE")) {
            type = DbObject.ROLE;
        } else if (readIf("SCHEMA")) {
            type = DbObject.SCHEMA;
        } else if (readIf("SEQUENCE")) {
            type = DbObject.SEQUENCE;
        } else if (readIf("TRIGGER")) {
            type = DbObject.TRIGGER;
        } else if (readIf("USER")) {
            type = DbObject.USER;
        } else if (readIf("DOMAIN")) {
            type = DbObject.USER_DATATYPE;
        } else {
            throw getSyntaxError();
        }
        SetComment command = new SetComment(session);
        String objectName = readIdentifierWithSchema();
        if (column) {
            String columnName = objectName;
            objectName = schemaName;
            schemaName = session.getCurrentSchemaName();
            if (readIf(".")) {
                schemaName = objectName;
                objectName = columnName;
                columnName = readUniqueIdentifier();
            }
            command.setColumn(true);
            command.setColumnName(columnName);
        }
        command.setSchemaName(schemaName);
        command.setObjectName(objectName);
        command.setObjectType(type);
        read("IS");
        command.setCommentExpression(readExpression());
        return command;
    }

    private Prepared parseDrop() throws SQLException {
        if (readIf("TABLE")) {
            boolean ifExists = readIfExists(false);
            String tableName = readIdentifierWithSchema();
            DropTable command = new DropTable(session, getSchema());
            command.setTableName(tableName);
            while (readIf(",")) {
                tableName = readIdentifierWithSchema();
                DropTable next = new DropTable(session, getSchema());
                next.setTableName(tableName);
                command.addNextDropTable(next);
            }
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            if (readIf("CASCADE")) {
                readIf("CONSTRAINTS");
            }
            return command;
        } else if (readIf("INDEX")) {
            boolean ifExists = readIfExists(false);
            String indexName = readIdentifierWithSchema();
            DropIndex command = new DropIndex(session, getSchema());
            command.setIndexName(indexName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("USER")) {
            boolean ifExists = readIfExists(false);
            DropUser command = new DropUser(session);
            command.setUserName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            readIf("CASCADE");
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("SEQUENCE")) {
            boolean ifExists = readIfExists(false);
            String sequenceName = readIdentifierWithSchema();
            DropSequence command = new DropSequence(session, getSchema());
            command.setSequenceName(sequenceName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("CONSTANT")) {
            boolean ifExists = readIfExists(false);
            String constantName = readIdentifierWithSchema();
            DropConstant command = new DropConstant(session, getSchema());
            command.setConstantName(constantName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("TRIGGER")) {
            boolean ifExists = readIfExists(false);
            String triggerName = readIdentifierWithSchema();
            DropTrigger command = new DropTrigger(session, getSchema());
            command.setTriggerName(triggerName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("VIEW")) {
            boolean ifExists = readIfExists(false);
            String viewName = readIdentifierWithSchema();
            DropView command = new DropView(session, getSchema());
            command.setViewName(viewName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("ROLE")) {
            boolean ifExists = readIfExists(false);
            DropRole command = new DropRole(session);
            command.setRoleName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
            // TODO role: support role names SELECT | DELETE | INSERT | UPDATE |
            // ALL? does quoting work?
        } else if (readIf("ALIAS")) {
            boolean ifExists = readIfExists(false);
            DropFunctionAlias command = new DropFunctionAlias(session);
            command.setAliasName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("SCHEMA")) {
            boolean ifExists = readIfExists(false);
            DropSchema command = new DropSchema(session);
            command.setSchemaName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("ALL")) {
            read("OBJECTS");
            DropDatabase command = new DropDatabase(session);
            command.setDropAllObjects(true);
            if (readIf("DELETE")) {
                read("FILES");
                command.setDeleteFiles(true);
            }
            return command;
        } else if (readIf("DOMAIN")) {
            return parseDropUserDataType();
        } else if (readIf("TYPE")) {
            return parseDropUserDataType();
        } else if (readIf("DATATYPE")) {
            return parseDropUserDataType();
        } else if (readIf("AGGREGATE")) {
            return parseDropAggregate();
        }
        throw getSyntaxError();
    }

    private DropUserDataType parseDropUserDataType() throws SQLException {
        boolean ifExists = readIfExists(false);
        DropUserDataType command = new DropUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private DropAggregate parseDropAggregate() throws SQLException {
        boolean ifExists = readIfExists(false);
        DropAggregate command = new DropAggregate(session);
        command.setName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private TableFilter readJoin(TableFilter top, Select command, boolean fromOuter) throws SQLException {
        TableFilter last = top;
        while (true) {
            if (readIf("RIGHT")) {
                readIf("OUTER");
                read("JOIN");
                // the right hand side is the 'inner' table usually
                TableFilter newTop = readTableFilter(fromOuter);
                newTop = readJoin(newTop, command, true);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                newTop.addJoin(top, true, on);
                top = newTop;
                last = newTop;
            } else if (readIf("LEFT")) {
                readIf("OUTER");
                read("JOIN");
                TableFilter join = readTableFilter(true);
                top = readJoin(top, command, true);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, true, on);
                last = join;
            } else if (readIf("FULL")) {
                throw this.getSyntaxError();
            } else if (readIf("INNER")) {
                read("JOIN");
                TableFilter join = readTableFilter(fromOuter);
                top = readJoin(top, command, false);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            } else if (readIf("JOIN")) {
                TableFilter join = readTableFilter(fromOuter);
                top = readJoin(top, command, false);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            } else if (readIf("CROSS")) {
                read("JOIN");
                TableFilter join = readTableFilter(fromOuter);
                top.addJoin(join, fromOuter, null);
                last = join;
            } else if (readIf("NATURAL")) {
                read("JOIN");
                TableFilter join = readTableFilter(fromOuter);
                Column[] tableCols = last.getTable().getColumns();
                Column[] joinCols = join.getTable().getColumns();
                String tableSchema = last.getTable().getSchema().getName();
                String joinSchema = join.getTable().getSchema().getName();
                Expression on = null;
                for (Column tc : tableCols) {
                    String tableColumnName = tc.getName();
                    for (Column c : joinCols) {
                        String joinColumnName = c.getName();
                        if (tableColumnName.equals(joinColumnName)) {
                            join.addNaturalJoinColumn(c);
                            Expression tableExpr = new ExpressionColumn(database, tableSchema, last
                                    .getTableAlias(), tableColumnName);
                            Expression joinExpr = new ExpressionColumn(database, joinSchema, join
                                    .getTableAlias(), joinColumnName);
                            Expression equal = new Comparison(session, Comparison.EQUAL, tableExpr, joinExpr);
                            if (on == null) {
                                on = equal;
                            } else {
                                on = new ConditionAndOr(ConditionAndOr.AND, on, equal);
                            }
                        }
                    }
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            } else {
                break;
            }
        }
        return top;
    }

    private Prepared parseExecute() throws SQLException {
        ExecuteProcedure command = new ExecuteProcedure(session);
        String procedureName = readAliasIdentifier();
        Procedure p = session.getProcedure(procedureName);
        if (p == null) {
            throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1, procedureName);
        }
        command.setProcedure(p);
        if (readIf("(")) {
            for (int i = 0;; i++) {
                command.setExpression(i, readExpression());
                if (readIf(")")) {
                    break;
                }
                read(",");
            }
        }
        return command;
    }

    private DeallocateProcedure parseDeallocate() throws SQLException {
        readIf("PLAN");
        String procedureName = readAliasIdentifier();
        DeallocateProcedure command = new DeallocateProcedure(session);
        command.setProcedureName(procedureName);
        return command;
    }

    private ExplainPlan parseExplain() throws SQLException {
        ExplainPlan command = new ExplainPlan(session);
        readIf("PLAN");
        readIf("FOR");
        if (isToken("SELECT") || isToken("FROM") || isToken("(")) {
            command.setCommand(parseSelect());
        } else if (readIf("DELETE")) {
            command.setCommand(parseDelete());
        } else if (readIf("UPDATE")) {
            command.setCommand(parseUpdate());
        } else if (readIf("INSERT")) {
            command.setCommand(parseInsert());
        } else if (readIf("MERGE")) {
            command.setCommand(parseMerge());
        } else {
            throw getSyntaxError();
        }
        return command;
    }

    private Query parseSelect() throws SQLException {
        int paramIndex = parameters.size();
        Query command = parseSelectUnion();
        ObjectArray<Parameter> params = ObjectArray.newInstance();
        for (int i = paramIndex; i < parameters.size(); i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        command.init();
        return command;
    }

    private Query parseSelectUnion() throws SQLException {
        int start = lastParseIndex;
        Query command = parseSelectSub();
        return parseSelectUnionExtension(command, start, false);
    }

    private Query parseSelectUnionExtension(Query command, int start, boolean unionOnly) throws SQLException {
        while (true) {
            if (readIf("UNION")) {
                SelectUnion union = new SelectUnion(session, command);
                if (readIf("ALL")) {
                    union.setUnionType(SelectUnion.UNION_ALL);
                } else {
                    readIf("DISTINCT");
                    union.setUnionType(SelectUnion.UNION);
                }
                union.setRight(parseSelectSub());
                command = union;
            } else if (readIf("MINUS") || readIf("EXCEPT")) {
                SelectUnion union = new SelectUnion(session, command);
                union.setUnionType(SelectUnion.EXCEPT);
                union.setRight(parseSelectSub());
                command = union;
            } else if (readIf("INTERSECT")) {
                SelectUnion union = new SelectUnion(session, command);
                union.setUnionType(SelectUnion.INTERSECT);
                union.setRight(parseSelectSub());
                command = union;
            } else {
                break;
            }
        }
        if (!unionOnly) {
            parseEndOfQuery(command);
        }
        setSQL(command, null, start);
        return command;
    }

    private void parseEndOfQuery(Query command) throws SQLException {
        if (readIf("ORDER")) {
            read("BY");
            Select oldSelect = currentSelect;
            if (command instanceof Select) {
                currentSelect = (Select) command;
            }
            ObjectArray<SelectOrderBy> orderList = ObjectArray.newInstance();
            do {
                boolean canBeNumber = true;
                if (readIf("=")) {
                    canBeNumber = false;
                }
                SelectOrderBy order = new SelectOrderBy();
                Expression expr = readExpression();
                if (canBeNumber && expr instanceof ValueExpression && expr.getType() == Value.INT) {
                    order.columnIndexExpr = expr;
                } else if (expr instanceof Parameter) {
                    recompileAlways = true;
                    order.columnIndexExpr = expr;
                } else {
                    order.expression = expr;
                }
                if (readIf("DESC")) {
                    order.descending = true;
                } else {
                    readIf("ASC");
                }
                if (readIf("NULLS")) {
                    if (readIf("FIRST")) {
                        order.nullsFirst = true;
                    } else {
                        read("LAST");
                        order.nullsLast = true;
                    }
                }
                orderList.add(order);
            } while (readIf(","));
            command.setOrder(orderList);
            currentSelect = oldSelect;
        }
        if (database.getMode().supportOffsetFetch) {
            // make sure aggregate functions will not work here
            Select temp = currentSelect;
            currentSelect = null;

            // http://sqlpro.developpez.com/SQL2008/
            if (readIf("OFFSET")) {
                command.setOffset(readExpression().optimize(session));
                if (!readIf("ROW")) {
                    read("ROWS");
                }
            }
            if (readIf("FETCH")) {
                read("FIRST");
                if (readIf("ROW")) {
                    command.setLimit(ValueExpression.get(ValueInt.get(1)));
                } else {
                    Expression limit = readExpression().optimize(session);
                    command.setLimit(limit);
                    if (!readIf("ROW")) {
                        read("ROWS");
                    }
                }
                read("ONLY");
            }

            currentSelect = temp;
        }
        if (readIf("LIMIT")) {
            Select temp = currentSelect;
            // make sure aggregate functions will not work here
            currentSelect = null;
            Expression limit = readExpression().optimize(session);
            command.setLimit(limit);
            if (readIf("OFFSET")) {
                Expression offset = readExpression().optimize(session);
                command.setOffset(offset);
            } else if (readIf(",")) {
                // MySQL: [offset, ] rowcount
                Expression offset = limit;
                limit = readExpression().optimize(session);
                command.setOffset(offset);
                command.setLimit(limit);
            }
            if (readIf("SAMPLE_SIZE")) {
                command.setSampleSize(getPositiveInt());
            }
            currentSelect = temp;
        }
        if (readIf("FOR")) {
            if (readIf("UPDATE")) {
                if (readIf("OF")) {
                    do {
                        readIdentifierWithSchema();
                    } while (readIf(","));
                } else if (readIf("NOWAIT")) {
                    // TODO parser: select for update nowait: should not wait
                } else if (readIf("WITH")) {
                    // Hibernate / Derby support
                    read("RR");
                }
                command.setForUpdate(true);
            } else if (readIf("READ")) {
                read("ONLY");
                if (readIf("WITH")) {
                    read("RS");
                }
            }
        }
    }

    private Query parseSelectSub() throws SQLException {
        if (readIf("(")) {
            Query command = parseSelectUnion();
            read(")");
            return command;
        }
        Select select = parseSelectSimple();
        return select;
    }

    private void parseSelectSimpleFromPart(Select command) throws SQLException {
        do {
            TableFilter filter = readTableFilter(false);
            parseJoinTableFilter(filter, command);
        } while (readIf(","));
    }

    private void parseJoinTableFilter(TableFilter top, Select command) throws SQLException {
        top = readJoin(top, command, top.isJoinOuter());
        command.addTableFilter(top, true);
        boolean isOuter = false;
        while (true) {
            TableFilter join = top.getJoin();
            if (join == null) {
                break;
            }
            isOuter = isOuter | join.isJoinOuter();
            if (isOuter) {
                command.addTableFilter(join, false);
            } else {
                // make flat so the optimizer can work better
                Expression on = join.getJoinCondition();
                if (on != null) {
                    command.addCondition(on);
                }
                join.removeJoinCondition();
                top.removeJoin();
                command.addTableFilter(join, true);
            }
            top = join;
        }
    }

    private void parseSelectSimpleSelectPart(Select command) throws SQLException {
        Select temp = currentSelect;
        // make sure aggregate functions will not work in TOP and LIMIT
        currentSelect = null;
        if (readIf("TOP")) {
            // can't read more complex expressions here because
            // SELECT TOP 1 +? A FROM TEST could mean
            // SELECT TOP (1+?) A FROM TEST or
            // SELECT TOP 1 (+?) AS A FROM TEST
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        } else if (readIf("LIMIT")) {
            Expression offset = readTerm().optimize(session);
            command.setOffset(offset);
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        }
        currentSelect = temp;
        if (readIf("DISTINCT")) {
            command.setDistinct(true);
        } else {
            readIf("ALL");
        }
        ObjectArray<Expression> expressions = ObjectArray.newInstance();
        do {
            if (readIf("*")) {
                expressions.add(new Wildcard(null, null));
            } else {
                Expression expr = readExpression();
                if (readIf("AS") || currentTokenType == IDENTIFIER) {
                    String alias = readAliasIdentifier();
                    expr = new Alias(expr, alias, database.getMode().aliasColumnName);
                }
                expressions.add(expr);
            }
        } while (readIf(","));
        command.setExpressions(expressions);
    }

    private Select parseSelectSimple() throws SQLException {
        boolean fromFirst;
        if (readIf("SELECT")) {
            fromFirst = false;
        } else if (readIf("FROM")) {
            fromFirst = true;
        } else {
            throw getSyntaxError();
        }
        Select command = new Select(session);
        int start = lastParseIndex;
        Select oldSelect = currentSelect;
        currentSelect = command;
        currentPrepared = command;
        if (fromFirst) {
            parseSelectSimpleFromPart(command);
            read("SELECT");
            parseSelectSimpleSelectPart(command);
        } else {
            parseSelectSimpleSelectPart(command);
            if (!readIf("FROM")) {
                // select without FROM: convert to SELECT ... FROM
                // SYSTEM_RANGE(1,1)
                Table dual = getDualTable();
                TableFilter filter = new TableFilter(session, dual, null, rightsChecked, currentSelect);
                command.addTableFilter(filter, true);
            } else {
                parseSelectSimpleFromPart(command);
            }
        }
        if (readIf("WHERE")) {
            Expression condition = readExpression();
            command.addCondition(condition);
        }
        // the group by is read for the outer select (or not a select)
        // so that columns that are not grouped can be used
        currentSelect = oldSelect;
        if (readIf("GROUP")) {
            read("BY");
            command.setGroupQuery();
            ObjectArray<Expression> list = ObjectArray.newInstance();
            do {
                Expression expr = readExpression();
                list.add(expr);
            } while (readIf(","));
            command.setGroupBy(list);
        }
        currentSelect = command;
        if (readIf("HAVING")) {
            command.setGroupQuery();
            Expression condition = readExpression();
            command.setHaving(condition);
        }
        command.setParameterList(parameters);
        currentSelect = oldSelect;
        setSQL(command, "SELECT", start);
        return command;
    }

    private Table getDualTable() throws SQLException {
        Schema main = database.findSchema(Constants.SCHEMA_MAIN);
        Expression one = ValueExpression.get(ValueLong.get(1));
        return new RangeTable(main, one, one);
    }

    private void setSQL(Prepared command, String start, int startIndex) {
        String sql = originalSQL.substring(startIndex, lastParseIndex).trim();
        if (start != null) {
            sql = start + " " + sql;
        }
        command.setSQL(sql);
    }

    private Expression readExpression() throws SQLException {
        Expression r = readAnd();
        while (readIf("OR")) {
            r = new ConditionAndOr(ConditionAndOr.OR, r, readAnd());
        }
        return r;
    }

    private Expression readAnd() throws SQLException {
        Expression r = readCondition();
        while (readIf("AND")) {
            r = new ConditionAndOr(ConditionAndOr.AND, r, readCondition());
        }
        return r;
    }

    private Expression readCondition() throws SQLException {
        // TODO parser: should probably use switch case for performance
        if (readIf("NOT")) {
            return new ConditionNot(readCondition());
        }
        if (readIf("EXISTS")) {
            read("(");
            Query query = parseSelect();
            // can not reduce expression because it might be a union except
            // query with distinct
            read(")");
            return new ConditionExists(query);
        }
        Expression r = readConcat();
        while (true) {
            // special case: NOT NULL is not part of an expression (as in CREATE
            // TABLE TEST(ID INT DEFAULT 0 NOT NULL))
            int backup = parseIndex;
            boolean not = false;
            if (readIf("NOT")) {
                not = true;
                if (isToken("NULL")) {
                    // this really only works for NOT NULL!
                    parseIndex = backup;
                    currentToken = "NOT";
                    break;
                }
            }
            if (readIf("LIKE")) {
                Expression b = readConcat();
                Expression esc = null;
                if (readIf("ESCAPE")) {
                    esc = readConcat();
                }
                recompileAlways = true;
                r = new CompareLike(database.getCompareMode(), r, b, esc, false);
            } else if (readIf("REGEXP")) {
                Expression b = readConcat();
                r = new CompareLike(database.getCompareMode(), r, b, null, true);
            } else if (readIf("IS")) {
                int type;
                if (readIf("NOT")) {
                    type = Comparison.IS_NOT_NULL;
                } else {
                    type = Comparison.IS_NULL;
                }
                read("NULL");
                r = new Comparison(session, type, r, null);
            } else if (readIf("IN")) {
                if (SysProperties.OPTIMIZE_IN && !SysProperties.OPTIMIZE_IN_LIST) {
                    recompileAlways = true;
                }
                read("(");
                if (readIf(")")) {
                    r = ValueExpression.get(ValueBoolean.get(false));
                } else {
                    if (isToken("SELECT") || isToken("FROM")) {
                        Query query = parseSelect();
                        r = new ConditionInSelect(database, r, query, false, Comparison.EQUAL);
                    } else {
                        ObjectArray<Expression> v = ObjectArray.newInstance();
                        Expression last;
                        do {
                            last = readExpression();
                            v.add(last);
                        } while (readIf(","));
                        if (v.size() == 1 && (last instanceof Subquery)) {
                            Subquery s = (Subquery) last;
                            Query q = s.getQuery();
                            r = new ConditionInSelect(database, r, q, false, Comparison.EQUAL);
                        } else {
                            r = new ConditionIn(database, r, v);
                        }
                    }
                    read(")");
                }
            } else if (readIf("BETWEEN")) {
                Expression low = readConcat();
                read("AND");
                Expression high = readConcat();
                Expression condLow = new Comparison(session, Comparison.SMALLER_EQUAL, low, r);
                Expression condHigh = new Comparison(session, Comparison.BIGGER_EQUAL, high, r);
                r = new ConditionAndOr(ConditionAndOr.AND, condLow, condHigh);
            } else {
                int compareType = getCompareType(currentTokenType);
                if (compareType < 0) {
                    break;
                }
                read();
                if (readIf("ALL")) {
                    read("(");
                    Query query = parseSelect();
                    r = new ConditionInSelect(database, r, query, true, compareType);
                    read(")");
                } else if (readIf("ANY") || readIf("SOME")) {
                    read("(");
                    Query query = parseSelect();
                    r = new ConditionInSelect(database, r, query, false, compareType);
                    read(")");
                } else {
                    Expression right = readConcat();
                    if (readIf("(") && readIf("+") && readIf(")")) {
                        // support for a subset of old-fashioned Oracle outer
                        // join with (+)
                        if (r instanceof ExpressionColumn && right instanceof ExpressionColumn) {
                            ExpressionColumn leftCol = (ExpressionColumn) r;
                            ExpressionColumn rightCol = (ExpressionColumn) right;
                            ObjectArray<TableFilter> filters = currentSelect.getTopFilters();
                            for (TableFilter f : filters) {
                                while (f != null) {
                                    leftCol.mapColumns(f, 0);
                                    rightCol.mapColumns(f, 0);
                                    f = f.getJoin();
                                }
                            }
                            TableFilter leftFilter = leftCol.getTableFilter();
                            TableFilter rightFilter = rightCol.getTableFilter();
                            r = new Comparison(session, compareType, r, right);
                            if (leftFilter != null && rightFilter != null) {
                                int idx = filters.indexOf(rightFilter);
                                if (idx >= 0) {
                                    filters.remove(idx);
                                    leftFilter.addJoin(rightFilter, true, r);
                                } else {
                                    rightFilter.mapAndAddFilter(r);
                                }
                                r = ValueExpression.get(ValueBoolean.get(true));
                            }
                        }
                    } else {
                        r = new Comparison(session, compareType, r, right);
                    }
                }
            }
            if (not) {
                r = new ConditionNot(r);
            }
        }
        return r;
    }

    private Expression readConcat() throws SQLException {
        Expression r = readSum();
        while (true) {
            if (readIf("||")) {
                r = new Operation(Operation.CONCAT, r, readSum());
            } else if (readIf("~")) {
                if (readIf("*")) {
                    Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X", Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new CompareLike(database.getCompareMode(), r, readSum(), null, true);
            } else if (readIf("!~")) {
                if (readIf("*")) {
                    Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X", Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new ConditionNot(new CompareLike(database.getCompareMode(), r, readSum(), null, true));
            } else {
                return r;
            }
        }
    }

    private Expression readSum() throws SQLException {
        Expression r = readFactor();
        while (true) {
            if (readIf("+")) {
                r = new Operation(Operation.PLUS, r, readFactor());
            } else if (readIf("-")) {
                r = new Operation(Operation.MINUS, r, readFactor());
            } else {
                return r;
            }
        }
    }

    private Expression readFactor() throws SQLException {
        Expression r = readTerm();
        while (true) {
            if (readIf("*")) {
                r = new Operation(Operation.MULTIPLY, r, readTerm());
            } else if (readIf("/")) {
                r = new Operation(Operation.DIVIDE, r, readTerm());
            } else {
                return r;
            }
        }
    }

    private Expression readAggregate(int aggregateType) throws SQLException {
        if (currentSelect == null) {
            throw getSyntaxError();
        }
        currentSelect.setGroupQuery();
        Expression r;
        if (aggregateType == Aggregate.COUNT) {
            if (readIf("*")) {
                r = new Aggregate(Aggregate.COUNT_ALL, null, currentSelect, false);
            } else {
                boolean distinct = readIf("DISTINCT");
                Expression on = readExpression();
                if (on instanceof Wildcard && !distinct) {
                    // PostgreSQL compatibility: count(t.*)
                    r = new Aggregate(Aggregate.COUNT_ALL, null, currentSelect, false);
                } else {
                    r = new Aggregate(Aggregate.COUNT, on, currentSelect, distinct);
                }
            }
        } else if (aggregateType == Aggregate.GROUP_CONCAT) {
            boolean distinct = readIf("DISTINCT");
            Aggregate agg = new Aggregate(Aggregate.GROUP_CONCAT, readExpression(), currentSelect, distinct);
            if (readIf("ORDER")) {
                read("BY");
                agg.setOrder(parseSimpleOrderList());
            }
            if (readIf("SEPARATOR")) {
                agg.setSeparator(readExpression());
            }
            r = agg;
        } else {
            boolean distinct = readIf("DISTINCT");
            r = new Aggregate(aggregateType, readExpression(), currentSelect, distinct);
        }
        read(")");
        return r;
    }

    private ObjectArray<SelectOrderBy> parseSimpleOrderList() throws SQLException {
        ObjectArray<SelectOrderBy> orderList = ObjectArray.newInstance();
        do {
            SelectOrderBy order = new SelectOrderBy();
            Expression expr = readExpression();
            order.expression = expr;
            if (readIf("DESC")) {
                order.descending = true;
            } else {
                readIf("ASC");
            }
            orderList.add(order);
        } while (readIf(","));
        return orderList;
    }

    private JavaFunction readJavaFunction(String name) throws SQLException {
        FunctionAlias functionAlias = database.findFunctionAlias(name);
        if (functionAlias == null) {
            throw Message.getSQLException(ErrorCode.FUNCTION_NOT_FOUND_1, name);
        }
        Expression[] args;
        ObjectArray<Expression> argList = ObjectArray.newInstance();
        int numArgs = 0;
        while (!readIf(")")) {
            if (numArgs++ > 0) {
                read(",");
            }
            argList.add(readExpression());
        }
        args = new Expression[numArgs];
        argList.toArray(args);
        JavaFunction func = new JavaFunction(functionAlias, args);
        return func;
    }

    private JavaAggregate readJavaAggregate(UserAggregate aggregate) throws SQLException {
        ObjectArray<Expression> params = ObjectArray.newInstance();
        do {
            params.add(readExpression());
        } while(readIf(","));
        read(")");
        Expression[] list = new Expression[params.size()];
        params.toArray(list);
        JavaAggregate agg = new JavaAggregate(aggregate, list, currentSelect);
        currentSelect.setGroupQuery();
        return agg;
    }

    private Expression readFunction(String name) throws SQLException {
        int agg = Aggregate.getAggregateType(name);
        if (agg >= 0) {
            return readAggregate(agg);
        }
        Function function = Function.getFunction(database, name);
        if (function == null) {
            UserAggregate aggregate = database.findAggregate(name);
            if (aggregate != null) {
                return readJavaAggregate(aggregate);
            }
            return readJavaFunction(name);
        }
        switch (function.getFunctionType()) {
        case Function.CAST: {
            function.setParameter(0, readExpression());
            read("AS");
            Column type = parseColumn(null);
            function.setDataType(type);
            read(")");
            break;
        }
        case Function.CONVERT: {
            function.setParameter(0, readExpression());
            read(",");
            Column type = parseColumn(null);
            function.setDataType(type);
            read(")");
            break;
        }
        case Function.EXTRACT: {
            function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
            read();
            read("FROM");
            function.setParameter(1, readExpression());
            read(")");
            break;
        }
        case Function.DATE_DIFF: {
            if (Function.isDatePart(currentToken)) {
                function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
                read();
            } else {
                function.setParameter(0, readExpression());
            }
            read(",");
            function.setParameter(1, readExpression());
            read(",");
            function.setParameter(2, readExpression());
            read(")");
            break;
        }
        case Function.SUBSTRING: {
            function.setParameter(0, readExpression());
            if (!readIf(",")) {
                read("FROM");
            }
            function.setParameter(1, readExpression());
            if (readIf("FOR") || readIf(",")) {
                function.setParameter(2, readExpression());
            }
            read(")");
            break;
        }
        case Function.POSITION: {
            // can't read expression because IN would be read too early
            function.setParameter(0, readConcat());
            if (!readIf(",")) {
                read("IN");
            }
            function.setParameter(1, readExpression());
            read(")");
            break;
        }
        case Function.TRIM: {
            Expression space = null;
            if (readIf("LEADING")) {
                function = Function.getFunction(database, "LTRIM");
                if (!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            } else if (readIf("TRAILING")) {
                function = Function.getFunction(database, "RTRIM");
                if (!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            } else if (readIf("BOTH")) {
                if (!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            }
            Expression p0 = readExpression();
            if (readIf(",")) {
                space = readExpression();
            } else if (readIf("FROM")) {
                space = p0;
                p0 = readExpression();
            }
            function.setParameter(0, p0);
            if (space != null) {
                function.setParameter(1, space);
            }
            read(")");
            break;
        }
        case Function.TABLE:
        case Function.TABLE_DISTINCT: {
            int i = 0;
            ObjectArray<Column> columns = ObjectArray.newInstance();
            do {
                String columnName = readAliasIdentifier();
                Column column = parseColumn(columnName);
                columns.add(column);
                read("=");
                function.setParameter(i, readExpression());
                i++;
            } while (readIf(","));
            read(")");
            TableFunction tf = (TableFunction) function;
            tf.setColumns(columns);
            break;
        }
        default:
            if (!readIf(")")) {
                int i = 0;
                do {
                    function.setParameter(i++, readExpression());
                } while (readIf(","));
                read(")");
            }
        }
        function.doneWithParameters();
        return function;
    }

    private Function readFunctionWithoutParameters(String name) throws SQLException {
        if (readIf("(")) {
            read(")");
        }
        Function function = Function.getFunction(database, name);
        function.doneWithParameters();
        return function;
    }

    private Expression readWildcardOrSequenceValue(String schema, String objectName) throws SQLException {
        if (readIf("*")) {
            return new Wildcard(schema, objectName);
        }
        if (schema == null) {
            schema = session.getCurrentSchemaName();
        }
        if (readIf("NEXTVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                return new SequenceValue(sequence);
            }
        } else if (readIf("CURRVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                Function function = Function.getFunction(database, "CURRVAL");
                function.setParameter(0, ValueExpression.get(ValueString.get(sequence.getSchema().getName())));
                function.setParameter(1, ValueExpression.get(ValueString.get(sequence.getName())));
                function.doneWithParameters();
                return function;
            }
        }
        return null;
    }

    private Expression readTermObjectDot(String objectName) throws SQLException {
        Expression expr = readWildcardOrSequenceValue(null, objectName);
        if (expr != null) {
            return expr;
        }
        String name = readColumnIdentifier();
        if (readIf(".")) {
            String schema = objectName;
            objectName = name;
            expr = readWildcardOrSequenceValue(schema, objectName);
            if (expr != null) {
                return expr;
            }
            name = readColumnIdentifier();
            if (readIf(".")) {
                String databaseName = schema;
                if (!database.getShortName().equals(databaseName)) {
                    throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, databaseName);
                }
                schema = objectName;
                objectName = name;
                expr = readWildcardOrSequenceValue(schema, objectName);
                if (expr != null) {
                    return expr;
                }
                name = readColumnIdentifier();
                return new ExpressionColumn(database, schema, objectName, name);
            }
            return new ExpressionColumn(database, schema, objectName, name);
        }
        return new ExpressionColumn(database, null, objectName, name);
    }

    private Expression readTerm() throws SQLException {
        Expression r;
        switch (currentTokenType) {
        case AT:
            read();
            r = new Variable(session, readAliasIdentifier());
            if (readIf(":=")) {
                Expression value = readExpression();
                Function function = Function.getFunction(database, "SET");
                function.setParameter(0, r);
                function.setParameter(1, value);
                r = function;
            }
            break;
        case PARAMETER:
            // there must be no space between ? and the number
            boolean indexed = Character.isDigit(sqlCommandChars[parseIndex]);
            read();
            Parameter p;
            if (indexed && currentTokenType == VALUE && currentValue.getType() == Value.INT) {
                if (indexedParameterList == null) {
                    if (parameters == null) {
                        // this can occur when parsing expressions only (for example check constraints)
                        throw getSyntaxError();
                    } else if (parameters.size() > 0) {
                        throw Message.getSQLException(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                    }
                    indexedParameterList = ObjectArray.newInstance();
                }
                int index = currentValue.getInt() - 1;
                if (index < 0 || index >= Constants.MAX_PARAMETER_INDEX) {
                    throw Message.getInvalidValueException("" + index, "Parameter Index");
                }
                if (indexedParameterList.size() <= index) {
                    indexedParameterList.setSize(index + 1);
                }
                p = indexedParameterList.get(index);
                if (p == null) {
                    p = new Parameter(index);
                    indexedParameterList.set(index, p);
                }
                read();
            } else {
                if (indexedParameterList != null) {
                    throw Message.getSQLException(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                }
                p = new Parameter(parameters.size());
            }
            parameters.add(p);
            r = p;
            break;
        case KEYWORD:
            if (isToken("SELECT") || isToken("FROM")) {
                Query query = parseSelect();
                r = new Subquery(query);
            } else {
                throw getSyntaxError();
            }
            break;
        case IDENTIFIER:
            String name = currentToken;
            if (currentTokenQuoted) {
                read();
                if (readIf("(")) {
                    r = readFunction(name);
                } else if (readIf(".")) {
                    r = readTermObjectDot(name);
                } else {
                    r = new ExpressionColumn(database, null, null, name);
                }
            } else {
                read();
                if ("X".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                    read();
                    byte[] buffer = ByteUtils.convertStringToBytes(currentValue.getString());
                    r = ValueExpression.get(ValueBytes.getNoCopy(buffer));
                } else if (readIf(".")) {
                    r = readTermObjectDot(name);
                } else if ("CASE".equals(name)) {
                    // CASE must be processed before (,
                    // otherwise CASE(3) would be a function call, which it is
                    // not
                    if (isToken("WHEN")) {
                        r = readWhen(null);
                    } else {
                        Expression left = readExpression();
                        r = readWhen(left);
                    }
                } else if (readIf("(")) {
                    r = readFunction(name);
                } else if ("CURRENT_USER".equals(name)) {
                    r = readFunctionWithoutParameters("USER");
                } else if ("CURRENT".equals(name)) {
                    if (readIf("TIMESTAMP")) {
                        r = readFunctionWithoutParameters("CURRENT_TIMESTAMP");
                    } else if (readIf("TIME")) {
                        r = readFunctionWithoutParameters("CURRENT_TIME");
                    } else if (readIf("DATE")) {
                        r = readFunctionWithoutParameters("CURRENT_DATE");
                    } else {
                        r = new ExpressionColumn(database, null, null, name);
                    }
                } else if ("NEXT".equals(name) && readIf("VALUE")) {
                    read("FOR");
                    Sequence sequence = readSequence();
                    r = new SequenceValue(sequence);
                } else if ("DATE".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                    String date = currentValue.getString();
                    read();
                    r = ValueExpression.get(ValueDate.get(ValueDate.parseDate(date)));
                } else if ("TIME".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                    String time = currentValue.getString();
                    read();
                    r = ValueExpression.get(ValueTime.get(ValueTime.parseTime(time)));
                } else if ("TIMESTAMP".equals(name) && currentTokenType == VALUE
                        && currentValue.getType() == Value.STRING) {
                    String timestamp = currentValue.getString();
                    read();
                    r = ValueExpression.get(ValueTimestamp.getNoCopy(ValueTimestamp.parseTimestamp(timestamp)));
                } else if ("E".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                    String text = currentValue.getString();
                    read();
                    r = ValueExpression.get(ValueString.get(text));
                } else {
                    r = new ExpressionColumn(database, null, null, name);
                }
            }
            break;
        case MINUS:
            read();
            if (currentTokenType == VALUE) {
                r = ValueExpression.get(currentValue.negate());
                // convert Integer.MIN_VALUE to int (-Integer.MIN_VALUE needed
                // to be a long)
                if (r.getType() == Value.LONG && r.getValue(session).getLong() == Integer.MIN_VALUE) {
                    r = ValueExpression.get(ValueInt.get(Integer.MIN_VALUE));
                }
                read();
            } else {
                r = new Operation(Operation.NEGATE, readTerm(), null);
            }
            break;
        case PLUS:
            read();
            r = readTerm();
            break;
        case OPEN:
            read();
            r = readExpression();
            if (readIf(",")) {
                ObjectArray<Expression> list = ObjectArray.newInstance();
                list.add(r);
                do {
                    r = readExpression();
                    list.add(r);
                } while (readIf(","));
                Expression[] array = new Expression[list.size()];
                list.toArray(array);
                r = new ExpressionList(array);
            }
            read(")");
            break;
        case TRUE:
            read();
            r = ValueExpression.get(ValueBoolean.get(true));
            break;
        case FALSE:
            read();
            r = ValueExpression.get(ValueBoolean.get(false));
            break;
        case CURRENT_TIME:
            read();
            r = readFunctionWithoutParameters("CURRENT_TIME");
            break;
        case CURRENT_DATE:
            read();
            r = readFunctionWithoutParameters("CURRENT_DATE");
            break;
        case CURRENT_TIMESTAMP: {
            Function function = Function.getFunction(database, "CURRENT_TIMESTAMP");
            read();
            if (readIf("(")) {
                if (!readIf(")")) {
                    function.setParameter(0, readExpression());
                    read(")");
                }
            }
            function.doneWithParameters();
            r = function;
            break;
        }
        case ROWNUM:
            read();
            if (readIf("(")) {
                read(")");
            }
            r = new Rownum(currentSelect == null ? currentPrepared : currentSelect);
            break;
        case NULL:
            read();
            r = ValueExpression.getNull();
            break;
        case VALUE:
            r = ValueExpression.get(currentValue);
            read();
            break;
        default:
            throw getSyntaxError();
        }
        if (readIf("[")) {
            Function function = Function.getFunction(database, "ARRAY_GET");
            function.setParameter(0, r);
            r = readExpression();
            r = new Operation(Operation.PLUS, r, ValueExpression.get(ValueInt.get(1)));
            function.setParameter(1, r);
            r = function;
            read("]");
        }
        if (readIf("::")) {
            // PostgreSQL compatibility
            Column col = parseColumn(null);
            Function function = Function.getFunction(database, "CAST");
            function.setDataType(col);
            function.setParameter(0, r);
            r = function;
        }
        return r;
    }

    private Expression readWhen(Expression left) throws SQLException {
        if (readIf("END")) {
            readIf("CASE");
            return ValueExpression.getNull();
        }
        if (readIf("ELSE")) {
            Expression elsePart = readExpression();
            read("END");
            readIf("CASE");
            return elsePart;
        }
        readIf("WHEN");
        Expression when = readExpression();
        if (left != null) {
            when = new Comparison(session, Comparison.EQUAL, left, when);
        }
        read("THEN");
        Expression then = readExpression();
        Expression elsePart = readWhen(left);
        Function function = Function.getFunction(session.getDatabase(), "CASEWHEN");
        function.setParameter(0, when);
        function.setParameter(1, then);
        function.setParameter(2, elsePart);
        function.doneWithParameters();
        return function;
    }

    private int getPositiveInt() throws SQLException {
        int v = getInt();
        if (v < 0) {
            throw Message.getInvalidValueException("" + v, "positive integer");
        }
        return v;
    }

    private int getInt() throws SQLException {
        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS) {
            read();
        }
        if (currentTokenType != VALUE || currentValue.getType() != Value.INT) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "integer");
        }
        int i = currentValue.getInt();
        read();
        return minus ? -i : i;
    }

    private long readLong() throws SQLException {
        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        }
        if (currentTokenType != VALUE
                || (currentValue.getType() != Value.INT && currentValue.getType() != Value.DECIMAL)) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "long");
        }
        long i = currentValue.getLong();
        read();
        return minus ? -i : i;
    }

    private boolean readBooleanSetting() throws SQLException {
        if (currentTokenType == VALUE) {
            boolean result = currentValue.getBoolean().booleanValue();
            read();
            return result;
        }
        if (readIf("TRUE") || readIf("ON")) {
            return true;
        } else if (readIf("FALSE") || readIf("OFF")) {
            return false;
        } else {
            throw getSyntaxError();
        }
    }

    private String readString() throws SQLException {
        Expression expr = readExpression().optimize(session);
        if (!(expr instanceof ValueExpression)) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "string");
        }
        String s = expr.getValue(session).getString();
        return s;
    }

    private String readIdentifierWithSchema(String defaultSchemaName) throws SQLException {
        if (currentTokenType != IDENTIFIER) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier");
        }
        String s = currentToken;
        read();
        schemaName = defaultSchemaName;
        if (readIf(".")) {
            schemaName = s;
            if (currentTokenType != IDENTIFIER) {
                throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier");
            }
            s = currentToken;
            read();
        }
        if (".".equals(currentToken)) {
            if (schemaName.equalsIgnoreCase(database.getShortName())) {
                read(".");
                schemaName = s;
                if (currentTokenType != IDENTIFIER) {
                    throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier");
                }
                s = currentToken;
                read();
            }
        }
        return s;
    }

    private String readIdentifierWithSchema() throws SQLException {
        return readIdentifierWithSchema(session.getCurrentSchemaName());
    }

    private String readAliasIdentifier() throws SQLException {
        return readColumnIdentifier();
    }

    private String readUniqueIdentifier() throws SQLException {
        return readColumnIdentifier();
    }

    private String readColumnIdentifier() throws SQLException {
        if (currentTokenType != IDENTIFIER) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier");
        }
        String s = currentToken;
        read();
        return s;
    }

    private void read(String expected) throws SQLException {
        if (!expected.equals(currentToken) || currentTokenQuoted) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, expected);
        }
        read();
    }

    private boolean readIf(String token) throws SQLException {
        if (token.equals(currentToken) && !currentTokenQuoted) {
            read();
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean isToken(String token) {
        boolean result = token.equals(currentToken) && !currentTokenQuoted;
        if (result) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private void addExpected(String token) {
        if (expectedList != null) {
            expectedList.add(token);
        }
    }

    private void read() throws SQLException {
        currentTokenQuoted = false;
        if (expectedList != null) {
            expectedList.clear();
        }
        int[] types = characterTypes;
        lastParseIndex = parseIndex;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {
            type = types[++i];
        }
        int start = i;
        char[] chars = sqlCommandChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
        case CHAR_NAME:
            while (true) {
                type = types[i];
                if (type != CHAR_NAME && type != CHAR_VALUE) {
                    break;
                }
                i++;
            }
            currentToken = StringCache.getNew(sqlCommand.substring(start, i));
            currentTokenType = getTokenType(currentToken);
            parseIndex = i;
            return;
        case CHAR_QUOTED: {
            String result = null;
            while (true) {
                for (int begin = i;; i++) {
                    if (chars[i] == '\"') {
                        if (result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin - 1, i);
                        }
                        break;
                    }
                }
                if (chars[++i] != '\"') {
                    break;
                }
                i++;
            }
            currentToken = StringCache.getNew(result);
            parseIndex = i;
            currentTokenQuoted = true;
            currentTokenType = IDENTIFIER;
            return;
        }
        case CHAR_SPECIAL_2:
            if (types[i] == CHAR_SPECIAL_2) {
                i++;
            }
            currentToken = sqlCommand.substring(start, i);
            currentTokenType = getSpecialType(currentToken);
            parseIndex = i;
            return;
        case CHAR_SPECIAL_1:
            currentToken = sqlCommand.substring(start, i);
            currentTokenType = getSpecialType(currentToken);
            parseIndex = i;
            return;
        case CHAR_VALUE:
            if (c == '0' && chars[i] == 'X') {
                // hex number
                long number = 0;
                start += 2;
                i++;
                while (true) {
                    c = chars[i];
                    if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
                        checkLiterals(false);
                        currentValue = ValueInt.get((int) number);
                        currentTokenType = VALUE;
                        currentToken = "0";
                        parseIndex = i;
                        return;
                    }
                    number = (number << 4) + c - (c >= 'A' ? ('A' - 0xa) : ('0'));
                    if (number > Integer.MAX_VALUE) {
                        readHexDecimal(start, i);
                        return;
                    }
                    i++;
                }
            }
            long number = c - '0';
            while (true) {
                c = chars[i];
                if (c < '0' || c > '9') {
                    if (c == '.') {
                        readDecimal(start, i);
                        break;
                    }
                    if (c == 'E') {
                        readDecimal(start, i);
                        break;
                    }
                    checkLiterals(false);
                    currentValue = ValueInt.get((int) number);
                    currentTokenType = VALUE;
                    currentToken = "0";
                    parseIndex = i;
                    break;
                }
                number = number * 10 + (c - '0');
                if (number > Integer.MAX_VALUE) {
                    readDecimal(start, i);
                    break;
                }
                i++;
            }
            return;
        case CHAR_DECIMAL:
            if (types[i] != CHAR_VALUE) {
                currentTokenType = KEYWORD;
                currentToken = ".";
                parseIndex = i;
                return;
            }
            readDecimal(i - 1, i);
            return;
        case CHAR_STRING: {
            String result = null;
            while (true) {
                for (int begin = i;; i++) {
                    if (chars[i] == '\'') {
                        if (result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin - 1, i);
                        }
                        break;
                    }
                }
                if (chars[++i] != '\'') {
                    break;
                }
                i++;
            }
            currentToken = "'";
            checkLiterals(true);
            currentValue = ValueString.get(StringCache.getNew(result));
            parseIndex = i;
            currentTokenType = VALUE;
            return;
        }
        case CHAR_DOLLAR_QUOTED_STRING: {
            String result = null;
            int begin = i - 1;
            while (types[i] == CHAR_DOLLAR_QUOTED_STRING) {
                i++;
            }
            result = sqlCommand.substring(begin, i);
            currentToken = "'";
            checkLiterals(true);
            currentValue = ValueString.get(StringCache.getNew(result));
            parseIndex = i;
            currentTokenType = VALUE;
            return;
        }
        case CHAR_END:
            currentToken = "";
            currentTokenType = END;
            parseIndex = i;
            return;
        default:
            throw getSyntaxError();
        }
    }

    private void checkLiterals(boolean text) throws SQLException {
        if (!session.getAllowLiterals()) {
            int allowed = database.getAllowLiterals();
            if (allowed == Constants.ALLOW_LITERALS_NONE || (text && allowed != Constants.ALLOW_LITERALS_ALL)) {
                throw Message.getSQLException(ErrorCode.LITERALS_ARE_NOT_ALLOWED);
            }
        }
    }

    private void readHexDecimal(int start, int i) throws SQLException {
        char[] chars = sqlCommandChars;
        char c;
        do {
            c = chars[++i];
        } while ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'));
        parseIndex = i;
        String sub = sqlCommand.substring(start, i);
        BigDecimal bd = new BigDecimal(new BigInteger(sub, 16));
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    private void readDecimal(int start, int i) throws SQLException {
        char[] chars = sqlCommandChars;
        int[] types = characterTypes;
        // go until the first non-number
        while (true) {
            int t = types[i];
            if (t != CHAR_DECIMAL && t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        if (chars[i] == 'E') {
            i++;
            if (chars[i] == '+' || chars[i] == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) {
                throw getSyntaxError();
            }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        String sub = sqlCommand.substring(start, i);
        BigDecimal bd;
        try {
            bd = new BigDecimal(sub);
        } catch (NumberFormatException e) {
            throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, e, sub);
        }
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    public Session getSession() {
        return session;
    }

    private void initialize(String sql) throws SQLException {
        if (sql == null) {
            sql = "";
        }
        originalSQL = sql;
        sqlCommand = sql;
        int len = sql.length() + 1;
        char[] command = new char[len];
        int[] types = new int[len];
        len--;
        sql.getChars(0, len, command, 0);
        boolean changed = false;
        command[len] = ' ';
        int startLoop = 0;
        int lastType = 0;
        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
            case '/':
                if (command[i + 1] == '*') {
                    // block comment
                    changed = true;
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    startLoop = i;
                    i += 2;
                    checkRunOver(i, len, startLoop);
                    while (command[i] != '*' || command[i + 1] != '/') {
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    i++;
                } else if (command[i + 1] == '/') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len - 1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '-':
                if (command[i + 1] == '-') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len - 1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '$':
                if (SysProperties.DOLLAR_QUOTING && command[i + 1] == '$' && (i == 0 || command[i - 1] <= ' ')) {
                    // dollar quoted string
                    changed = true;
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    startLoop = i;
                    i += 2;
                    checkRunOver(i, len, startLoop);
                    while (command[i] != '$' || command[i + 1] != '$') {
                        types[i++] = CHAR_DOLLAR_QUOTED_STRING;
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    i++;
                } else {
                    if (lastType == CHAR_NAME || lastType == CHAR_VALUE) {
                        // $ inside an identifier is supported
                        type = CHAR_NAME;
                    } else {
                        // but not at the start, to support PostgreSQL $1
                        type = CHAR_SPECIAL_1;
                    }
                }
                break;
            case '(':
            case ')':
            case '{':
            case '}':
            case '*':
            case ',':
            case ';':
            case '+':
            case '%':
            case '?':
            case '@':
            case ']':
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '|':
            case '=':
            case ':':
            case '~':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DECIMAL;
                break;
            case '\'':
                type = types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '[':
                if (database.getMode().squareBracketQuotedNames) {
                    // SQL Server alias for "
                    command[i] = '"';
                    changed = true;
                    type = types[i] = CHAR_QUOTED;
                    startLoop = i;
                    while (command[++i] != ']') {
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = '"';
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '`':
                // MySQL alias for ", but not case sensitive
                command[i] = '"';
                changed = true;
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '`') {
                    checkRunOver(i, len, startLoop);
                    c = command[i];
                    command[i] = Character.toUpperCase(c);
                }
                command[i] = '"';
                break;
            case '\"':
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '\"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '_':
                type = CHAR_NAME;
                break;
            default:
                if (c >= 'a' && c <= 'z') {
                    command[i] = (char) (c - ('a' - 'A'));
                    changed = true;
                    type = CHAR_NAME;
                } else if (c >= 'A' && c <= 'Z') {
                    type = CHAR_NAME;
                } else if (c >= '0' && c <= '9') {
                    type = CHAR_VALUE;
                } else {
                    if (Character.isJavaIdentifierPart(c)) {
                        type = CHAR_NAME;
                        char u = Character.toUpperCase(c);
                        if (u != c) {
                            command[i] = u;
                            changed = true;
                        }
                    }
                }
            }
            types[i] = type;
            lastType = type;
        }
        sqlCommandChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        if (changed) {
            sqlCommand = new String(command);
        }
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) throws SQLException {
        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private int getSpecialType(String s) throws SQLException {
        char c0 = s.charAt(0);
        if (s.length() == 1) {
            switch (c0) {
            case '?':
            case '$':
                return PARAMETER;
            case '@':
                return AT;
            case '+':
                return PLUS;
            case '-':
                return MINUS;
            case '{':
            case '}':
            case '*':
            case '/':
            case ';':
            case ',':
            case ':':
            case '[':
            case ']':
            case '~':
                return KEYWORD;
            case '(':
                return OPEN;
            case ')':
                return CLOSE;
            case '<':
                return SMALLER;
            case '>':
                return BIGGER;
            case '=':
                return EQUAL;
            default:
                break;
            }
        } else if (s.length() == 2) {
            switch (c0) {
            case ':':
                if ("::".equals(s)) {
                    return KEYWORD;
                } else if (":=".equals(s)) {
                    return KEYWORD;
                }
                break;
            case '>':
                if (">=".equals(s)) {
                    return BIGGER_EQUAL;
                }
                break;
            case '<':
                if ("<=".equals(s)) {
                    return SMALLER_EQUAL;
                } else if ("<>".equals(s)) {
                    return NOT_EQUAL;
                }
                break;
            case '!':
                if ("!=".equals(s)) {
                    return NOT_EQUAL;
                } else if ("!~".equals(s)) {
                    return KEYWORD;
                }
                break;
            case '|':
                if ("||".equals(s)) {
                    return STRING_CONCAT;
                }
                break;
            }
        }
        throw getSyntaxError();
    }

    private int getTokenType(String s) throws SQLException {
        int len = s.length();
        if (len == 0) {
            throw getSyntaxError();
        }
        return getSaveTokenType(s, database.getMode().supportOffsetFetch);
    }

    /**
     * Checks if this string is a SQL keyword.
     *
     * @param s the token to check
     * @param supportOffsetFetch if OFFSET and FETCH are keywords
     * @return true if it is a keyword
     */
    public static boolean isKeyword(String s, boolean supportOffsetFetch) {
        if (s == null || s.length() == 0) {
            return false;
        }
        return getSaveTokenType(s, supportOffsetFetch) != IDENTIFIER;
    }

    private static int getSaveTokenType(String s, boolean supportOffsetFetch) {
        switch (s.charAt(0)) {
        case 'C':
            if (s.equals("CURRENT_TIMESTAMP")) {
                return CURRENT_TIMESTAMP;
            } else if (s.equals("CURRENT_TIME")) {
                return CURRENT_TIME;
            } else if (s.equals("CURRENT_DATE")) {
                return CURRENT_DATE;
            }
            return getKeywordOrIdentifier(s, "CROSS", KEYWORD);
        case 'D':
            return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
        case 'E':
            if ("EXCEPT".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
        case 'F':
            if ("FROM".equals(s)) {
                return KEYWORD;
            } else if ("FOR".equals(s)) {
                return KEYWORD;
            } else if ("FULL".equals(s)) {
                return KEYWORD;
            } else if (supportOffsetFetch && "FETCH".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "FALSE", FALSE);
        case 'G':
            return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
        case 'H':
            return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
        case 'I':
            if ("INNER".equals(s)) {
                return KEYWORD;
            } else if ("INTERSECT".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "IS", KEYWORD);
        case 'J':
            return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
        case 'L':
            if ("LIMIT".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
        case 'M':
            return getKeywordOrIdentifier(s, "MINUS", KEYWORD);
        case 'N':
            if ("NOT".equals(s)) {
                return KEYWORD;
            } else if ("NATURAL".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "NULL", NULL);
        case 'O':
            if ("ON".equals(s)) {
                return KEYWORD;
            } else if (supportOffsetFetch && "OFFSET".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
        case 'P':
            return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
        case 'R':
            return getKeywordOrIdentifier(s, "ROWNUM", ROWNUM);
        case 'S':
            if (s.equals("SYSTIMESTAMP")) {
                return CURRENT_TIMESTAMP;
            } else if (s.equals("SYSTIME")) {
                return CURRENT_TIME;
            } else if (s.equals("SYSDATE")) {
                return CURRENT_TIMESTAMP;
            }
            return getKeywordOrIdentifier(s, "SELECT", KEYWORD);
        case 'T':
            if ("TODAY".equals(s)) {
                return CURRENT_DATE;
            }
            return getKeywordOrIdentifier(s, "TRUE", TRUE);
        case 'U':
            if ("UNIQUE".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "UNION", KEYWORD);
        case 'W':
            return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
        default:
            return IDENTIFIER;
        }
    }

    private static int getKeywordOrIdentifier(String s1, String s2, int keywordType) {
        if (s1.equals(s2)) {
            return keywordType;
        }
        return IDENTIFIER;
    }

    private Column parseColumnForTable(String columnName) throws SQLException {
        Column column;
        boolean isIdentity = false;
        if (readIf("IDENTITY") || readIf("SERIAL")) {
            column = new Column(columnName, Value.LONG);
            column.setOriginalSQL("IDENTITY");
            parseAutoIncrement(column);
            column.setPrimaryKey(true);
        } else {
            column = parseColumn(columnName);
        }
        if (readIf("NOT")) {
            read("NULL");
            column.setNullable(false);
        } else {
            readIf("NULL");
            column.setNullable(true);
        }
        if (readIf("AS")) {
            if (isIdentity) {
                getSyntaxError();
            }
            Expression expr = readExpression();
            column.setComputedExpression(expr);
        } else if (readIf("DEFAULT")) {
            Expression defaultExpression = readExpression();
            column.setDefaultExpression(session, defaultExpression);
        } else if (readIf("GENERATED")) {
            if (!readIf("ALWAYS")) {
                read("BY");
                read("DEFAULT");
            }
            read("AS");
            read("IDENTITY");
            long start = 1, increment = 1;
            if (readIf("(")) {
                read("START");
                readIf("WITH");
                start = readLong();
                readIf(",");
                if (readIf("INCREMENT")) {
                    readIf("BY");
                    increment = readLong();
                }
                read(")");
            }
            column.setPrimaryKey(true);
            column.setAutoIncrement(true, start, increment);
        }
        if (readIf("NOT")) {
            read("NULL");
            column.setNullable(false);
        } else {
            readIf("NULL");
        }
        if (readIf("AUTO_INCREMENT")) {
            parseAutoIncrement(column);
            if (readIf("NOT")) {
                read("NULL");
            }
        } else if (readIf("IDENTITY")) {
            parseAutoIncrement(column);
            column.setPrimaryKey(true);
            if (readIf("NOT")) {
                read("NULL");
            }
        }
        if (readIf("NULL_TO_DEFAULT")) {
            column.setConvertNullToDefault(true);
        }
        if (readIf("SEQUENCE")) {
            Sequence sequence = readSequence();
            column.setSequence(sequence);
        }
        if (readIf("SELECTIVITY")) {
            int value = getPositiveInt();
            column.setSelectivity(value);
        }
        String comment = readCommentIf();
        if (comment != null) {
            column.setComment(comment);
        }
        return column;
    }

    private void parseAutoIncrement(Column column) throws SQLException {
        long start = 1, increment = 1;
        if (readIf("(")) {
            start = readLong();
            if (readIf(",")) {
                increment = readLong();
            }
            read(")");
        }
        column.setAutoIncrement(true, start, increment);
    }

    private String readCommentIf() throws SQLException {
        if (readIf("COMMENT")) {
            readIf("IS");
            return readString();
        }
        return null;
    }

    private Column parseColumn(String columnName) throws SQLException {
        String original = currentToken;
        boolean regular = false;
        if (readIf("LONG")) {
            if (readIf("RAW")) {
                original += " RAW";
            }
        } else if (readIf("DOUBLE")) {
            if (readIf("PRECISION")) {
                original += " PRECISION";
            }
        } else if (readIf("CHARACTER")) {
            if (readIf("VARYING")) {
                original += " VARYING";
            }
        } else {
            regular = true;
        }
        long precision = -1;
        int displaySize = -1;
        int scale = -1;
        String comment = null;
        Column templateColumn = null;
        DataType dataType;
        UserDataType userDataType = database.findUserDataType(original);
        if (userDataType != null) {
            templateColumn = userDataType.getColumn();
            dataType = DataType.getDataType(templateColumn.getType());
            comment = templateColumn.getComment();
            original = templateColumn.getOriginalSQL();
            precision = templateColumn.getPrecision();
            displaySize = templateColumn.getDisplaySize();
            scale = templateColumn.getScale();
        } else {
            dataType = DataType.getTypeByName(original);
            if (dataType == null) {
                throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, currentToken);
            }
        }
        if (database.getIgnoreCase() && dataType.type == Value.STRING && !"VARCHAR_CASESENSITIVE".equals(original)) {
            original = "VARCHAR_IGNORECASE";
            dataType = DataType.getTypeByName(original);
        }
        if (dataType.type == Value.NULL) {
            // We do support NULL in the database meta data,
            // but not actually when creating tables.
            throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, original);
        }
        if (regular) {
            read();
        }
        precision = precision == -1 ? dataType.defaultPrecision : precision;
        displaySize = displaySize == -1 ? dataType.defaultDisplaySize : displaySize;
        scale = scale == -1 ? dataType.defaultScale : scale;
        if (dataType.supportsPrecision || dataType.supportsScale) {
            if (readIf("(")) {
                long p = readLong();
                if (readIf("K")) {
                    p *= 1024;
                } else if (readIf("M")) {
                    p *= 1024 * 1024;
                } else if (readIf("G")) {
                    p *= 1024 * 1024 * 1024;
                }
                if (p > Long.MAX_VALUE) {
                    p = Long.MAX_VALUE;
                }
                original += "(" + p;
                // Oracle syntax
                readIf("CHAR");
                if (dataType.supportsScale) {
                    if (readIf(",")) {
                        scale = getInt();
                        original += ", " + scale;
                    } else {
                        // special case: TIMESTAMP(5) actually means TIMESTAMP(23, 5)
                        if (dataType.type == Value.TIMESTAMP) {
                            scale = MathUtils.convertLongToInt(p);
                            p = precision;
                        } else {
                            scale = 0;
                        }
                    }
                }
                precision = p;
                displaySize = MathUtils.convertLongToInt(precision);
                original += ")";
                read(")");
            }
        } else if (readIf("(")) {
            // Support for MySQL: INT(11), MEDIUMINT(8) and so on.
            // Just ignore the precision.
            getPositiveInt();
            read(")");
        }
        if (readIf("FOR")) {
            read("BIT");
            read("DATA");
            if (dataType.type == Value.STRING) {
                dataType = DataType.getTypeByName("BINARY");
            }
        }
        // MySQL compatibility
        readIf("UNSIGNED");
        int type = dataType.type;
        Column column = new Column(columnName, type, precision, scale, displaySize);
        if (templateColumn != null) {
            column.setNullable(templateColumn.isNullable());
            column.setDefaultExpression(session, templateColumn.getDefaultExpression());
            int selectivity = templateColumn.getSelectivity();
            if (selectivity != Constants.SELECTIVITY_DEFAULT) {
                column.setSelectivity(selectivity);
            }
            Expression checkConstraint = templateColumn.getCheckConstraint(session, columnName);
            if (checkConstraint != null) {
                column.addCheckConstraint(session, checkConstraint);
            }
        }
        column.setComment(comment);
        column.setOriginalSQL(original);
        return column;
    }

    private Prepared parseCreate() throws SQLException {
        boolean force = readIf("FORCE");
        if (readIf("LOCAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, false, force);
            }
            read("TABLE");
            return parseCreateTable(true, false, false);
        } else if (readIf("GLOBAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read("TABLE");
            return parseCreateTable(true, true, false);
        } else if (readIf("TEMP") || readIf("TEMPORARY")) {
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read("TABLE");
            return parseCreateTable(true, true, false);
        } else if (readIf("MEMORY")) {
            read("TABLE");
            return parseCreateTable(false, false, false);
        } else if (readIf("LINKED")) {
            return parseCreateLinkedTable(false, false, force);
        } else if (readIf("CACHED")) {
            read("TABLE");
            return parseCreateTable(false, false, true);
        } else if (readIf("TABLE")) {
            int defaultMode;
            Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_TABLE_TYPE));
            defaultMode = setting == null ? Constants.DEFAULT_TABLE_TYPE : setting.getIntValue();
            return parseCreateTable(false, false, defaultMode == Table.TYPE_CACHED);
        } else if (readIf("VIEW")) {
            return parseCreateView(force);
        } else if (readIf("ALIAS")) {
            return parseCreateFunctionAlias(force);
        } else if (readIf("SEQUENCE")) {
            return parseCreateSequence();
        } else if (readIf("USER")) {
            return parseCreateUser();
        } else if (readIf("TRIGGER")) {
            return parseCreateTrigger(force);
        } else if (readIf("ROLE")) {
            return parseCreateRole();
        } else if (readIf("SCHEMA")) {
            return parseCreateSchema();
        } else if (readIf("CONSTANT")) {
            return parseCreateConstant();
        } else if (readIf("DOMAIN")) {
            return parseCreateUserDataType();
        } else if (readIf("TYPE")) {
            return parseCreateUserDataType();
        } else if (readIf("DATATYPE")) {
            return parseCreateUserDataType();
        } else if (readIf("AGGREGATE")) {
            return parseCreateAggregate(force);
        } else {
            boolean hash = false, primaryKey = false, unique = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if (readIf("PRIMARY")) {
                read("KEY");
                if (readIf("HASH")) {
                    hash = true;
                }
                primaryKey = true;
            } else {
                if (readIf("UNIQUE")) {
                    unique = true;
                    if (readIf("HASH")) {
                        hash = true;
                    }
                }
                if (readIf("INDEX")) {
                    if (!isToken("ON")) {
                        ifNotExists = readIfNoExists();
                        indexName = readIdentifierWithSchema(null);
                        oldSchema = getSchema();
                    }
                } else {
                    throw getSyntaxError();
                }
            }
            read("ON");
            String tableName = readIdentifierWithSchema();
            checkSchema(oldSchema);
            CreateIndex command = new CreateIndex(session, getSchema());
            command.setIfNotExists(ifNotExists);
            command.setHash(hash);
            command.setPrimaryKey(primaryKey);
            command.setTableName(tableName);
            command.setUnique(unique);
            command.setIndexName(indexName);
            command.setComment(readCommentIf());
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            return command;
        }
    }

    private boolean addRoleOrRight(GrantRevoke command) throws SQLException {
        if (readIf("SELECT")) {
            command.addRight(Right.SELECT);
            return false;
        } else if (readIf("DELETE")) {
            command.addRight(Right.DELETE);
            return false;
        } else if (readIf("INSERT")) {
            command.addRight(Right.INSERT);
            return false;
        } else if (readIf("UPDATE")) {
            command.addRight(Right.UPDATE);
            return false;
        } else if (readIf("ALL")) {
            command.addRight(Right.ALL);
            return false;
        } else if (readIf("CONNECT")) {
            // ignore this right
            return false;
        } else if (readIf("RESOURCE")) {
            // ignore this right
            return false;
        } else {
            command.addRoleName(readUniqueIdentifier());
            return true;
        }
    }

    private GrantRevoke parseGrantRevoke(int operationType) throws SQLException {
        GrantRevoke command = new GrantRevoke(session);
        command.setOperationType(operationType);
        boolean isRoleBased = addRoleOrRight(command);
        while (readIf(",")) {
            boolean next = addRoleOrRight(command);
            if (next != isRoleBased) {
                throw Message.getSQLException(ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED);
            }
        }
        if (!isRoleBased) {
            if (readIf("ON")) {
                do {
                    Table table = readTableOrView();
                    command.addTable(table);
                } while (readIf(","));
            }
        }
        if (operationType == GrantRevoke.GRANT) {
            read("TO");
        } else {
            read("FROM");
        }
        command.setGranteeName(readUniqueIdentifier());
        return command;
    }

    private Call parserCall() throws SQLException {
        Call command = new Call(session);
        currentPrepared = command;
        command.setValue(readExpression());
        return command;
    }

    private CreateRole parseCreateRole() throws SQLException {
        CreateRole command = new CreateRole(session);
        command.setIfNotExists(readIfNoExists());
        command.setRoleName(readUniqueIdentifier());
        return command;
    }

    private CreateSchema parseCreateSchema() throws SQLException {
        CreateSchema command = new CreateSchema(session);
        command.setIfNotExists(readIfNoExists());
        command.setSchemaName(readUniqueIdentifier());
        if (readIf("AUTHORIZATION")) {
            command.setAuthorization(readUniqueIdentifier());
        } else {
            command.setAuthorization(session.getUser().getName());
        }
        return command;
    }

    private CreateSequence parseCreateSequence() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String sequenceName = readIdentifierWithSchema();
        CreateSequence command = new CreateSequence(session, getSchema());
        command.setIfNotExists(ifNotExists);
        command.setSequenceName(sequenceName);
        while (true) {
            if (readIf("START")) {
                readIf("WITH");
                command.setStartWith(readExpression());
            } else if (readIf("INCREMENT")) {
                readIf("BY");
                command.setIncrement(readExpression());
            } else if (readIf("CACHE")) {
                command.setCacheSize(readExpression());
            } else if (readIf("BELONGS_TO_TABLE")) {
                command.setBelongsToTable(true);
            } else {
                break;
            }
        }
        return command;
    }

    private boolean readIfNoExists() throws SQLException {
        if (readIf("IF")) {
            read("NOT");
            read("EXISTS");
            return true;
        }
        return false;
    }

    private CreateConstant parseCreateConstant() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String constantName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (isKeyword(constantName, false)) {
            throw Message.getSQLException(ErrorCode.CONSTANT_ALREADY_EXISTS_1, constantName);
        }
        read("VALUE");
        Expression expr = readExpression();
        CreateConstant command = new CreateConstant(session, schema);
        command.setConstantName(constantName);
        command.setExpression(expr);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateAggregate parseCreateAggregate(boolean force) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        CreateAggregate command = new CreateAggregate(session);
        command.setForce(force);
        String name = readUniqueIdentifier();
        if (isKeyword(name, false) || Function.getFunction(database, name) != null || Aggregate.getAggregateType(name) >= 0) {
            throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
        }
        command.setName(name);
        command.setIfNotExists(ifNotExists);
        read("FOR");
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private CreateUserDataType parseCreateUserDataType() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        CreateUserDataType command = new CreateUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        read("AS");
        Column col = parseColumnForTable("VALUE");
        if (readIf("CHECK")) {
            Expression expr = readExpression();
            col.addCheckConstraint(session, expr);
        }
        col.rename(null);
        command.setColumn(col);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateTrigger parseCreateTrigger(boolean force) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String triggerName = readIdentifierWithSchema(null);
        Schema schema = getSchema();
        boolean isBefore;
        if (readIf("BEFORE")) {
            isBefore = true;
        } else {
            read("AFTER");
            isBefore = false;
        }
        int typeMask = 0;
        do {
            if (readIf("INSERT")) {
                typeMask |= Trigger.INSERT;
            } else if (readIf("UPDATE")) {
                typeMask |= Trigger.UPDATE;
            } else if (readIf("DELETE")) {
                typeMask |= Trigger.DELETE;
            } else {
                throw getSyntaxError();
            }
        } while (readIf(","));
        read("ON");
        String tableName = readIdentifierWithSchema();
        checkSchema(schema);
        CreateTrigger command = new CreateTrigger(session, getSchema());
        command.setForce(force);
        command.setTriggerName(triggerName);
        command.setIfNotExists(ifNotExists);
        command.setBefore(isBefore);
        command.setTypeMask(typeMask);
        command.setTableName(tableName);
        if (readIf("FOR")) {
            read("EACH");
            read("ROW");
            command.setRowBased(true);
        } else {
            command.setRowBased(false);
        }
        if (readIf("QUEUE")) {
            command.setQueueSize(getPositiveInt());
        }
        command.setNoWait(readIf("NOWAIT"));
        read("CALL");
        command.setTriggerClassName(readUniqueIdentifier());
        return command;
    }

    private CreateUser parseCreateUser() throws SQLException {
        CreateUser command = new CreateUser(session);
        command.setIfNotExists(readIfNoExists());
        command.setUserName(readUniqueIdentifier());
        command.setComment(readCommentIf());
        if (readIf("PASSWORD")) {
            command.setPassword(readExpression());
        } else if (readIf("SALT")) {
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
        } else if (readIf("IDENTIFIED")) {
            read("BY");
            // uppercase if not quoted
            command.setPassword(ValueExpression.get(ValueString.get(readColumnIdentifier())));
        } else {
            throw getSyntaxError();
        }
        if (readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private CreateFunctionAlias parseCreateFunctionAlias(boolean force) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        CreateFunctionAlias command = new CreateFunctionAlias(session);
        command.setForce(force);
        String name = readUniqueIdentifier();
        if (isKeyword(name, false) || Function.getFunction(database, name) != null || Aggregate.getAggregateType(name) >= 0) {
            throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
        }
        command.setAliasName(name);
        command.setIfNotExists(ifNotExists);
        command.setDeterministic(readIf("DETERMINISTIC"));
        read("FOR");
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private Query parserWith() throws SQLException {
        String tempViewName = readIdentifierWithSchema();
        Schema schema = getSchema();
        TableData recursiveTable;
        read("(");
        ObjectArray<Column> columns = ObjectArray.newInstance();
        String[] cols = parseColumnList();
        for (String c : cols) {
            columns.add(new Column(c, Value.STRING));
        }
        int id = database.allocateObjectId(true, true);
        recursiveTable = schema.createTable(tempViewName, id, columns, true, false, true, false, Index.EMPTY_HEAD, session);
        session.addLocalTempTable(recursiveTable);
        String querySQL = StringCache.getNew(sqlCommand.substring(parseIndex));
        read("AS");
        Query withQuery = parseSelect();
        withQuery.prepare();
        session.removeLocalTempTable(recursiveTable);
        id = database.allocateObjectId(true, true);
        TableView view = new TableView(schema, id, tempViewName, querySQL, null, cols, session, true);
        view.setTemporary(true);
        // view.setOnCommitDrop(true);
        session.addLocalTempTable(view);
        Query query = parseSelect();
        query.prepare();
        query.setPrepareAlways(true);
        // session.removeLocalTempTable(view);
        return query;
    }

    private CreateView parseCreateView(boolean force) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String viewName = readIdentifierWithSchema();
        CreateView command = new CreateView(session, getSchema());
        this.prepared = command;
        command.setViewName(viewName);
        command.setIfNotExists(ifNotExists);
        command.setComment(readCommentIf());
        if (readIf("(")) {
            String[] cols = parseColumnList();
            command.setColumnNames(cols);
        }
        String select = StringCache.getNew(sqlCommand.substring(parseIndex));
        read("AS");
        try {
            Query query = parseSelect();
            query.prepare();
            command.setSelect(query);
        } catch (SQLException e) {
            if (force) {
                command.setSelectSQL(select);
            } else {
                throw e;
            }
        }
        return command;
    }

    private TransactionCommand parseCheckpoint() throws SQLException {
        TransactionCommand command;
        if (readIf("SYNC")) {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT_SYNC);
        } else {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT);
        }
        return command;
    }

    private Prepared parseAlter() throws SQLException {
        if (readIf("TABLE")) {
            return parseAlterTable();
        } else if (readIf("USER")) {
            return parseAlterUser();
        } else if (readIf("INDEX")) {
            return parseAlterIndex();
        } else if (readIf("SEQUENCE")) {
            return parseAlterSequence();
        } else if (readIf("VIEW")) {
            return parseAlterView();
        }
        throw getSyntaxError();
    }

    private void checkSchema(Schema old) throws SQLException {
        if (old != null && getSchema() != old) {
            throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH);
        }
    }

    private AlterIndexRename parseAlterIndex() throws SQLException {
        String indexName = readIdentifierWithSchema();
        Schema old = getSchema();
        AlterIndexRename command = new AlterIndexRename(session);
        command.setOldIndex(getSchema().getIndex(indexName));
        read("RENAME");
        read("TO");
        String newName = readIdentifierWithSchema(old.getName());
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private AlterView parseAlterView() throws SQLException {
        AlterView command = new AlterView(session);
        String viewName = readIdentifierWithSchema();
        Table tableView = getSchema().findTableOrView(session, viewName);
        if (!(tableView instanceof TableView)) {
            throw Message.getSQLException(ErrorCode.VIEW_NOT_FOUND_1, viewName);
        }
        TableView view = (TableView) tableView;
        command.setView(view);
        read("RECOMPILE");
        return command;
    }

    private AlterSequence parseAlterSequence() throws SQLException {
        String sequenceName = readIdentifierWithSchema();
        Sequence sequence = getSchema().getSequence(sequenceName);
        AlterSequence command = new AlterSequence(session, sequence.getSchema());
        command.setSequence(sequence);
        if (readIf("RESTART")) {
            read("WITH");
            command.setStartWith(readExpression());
        }
        if (readIf("INCREMENT")) {
            read("BY");
            command.setIncrement(readExpression());
        }
        return command;
    }

    private AlterUser parseAlterUser() throws SQLException {
        String userName = readUniqueIdentifier();
        if (readIf("SET")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(database.getUser(userName));
            if (readIf("PASSWORD")) {
                command.setPassword(readExpression());
            } else if (readIf("SALT")) {
                command.setSalt(readExpression());
                read("HASH");
                command.setHash(readExpression());
            } else {
                throw getSyntaxError();
            }
            return command;
        } else if (readIf("RENAME")) {
            read("TO");
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.RENAME);
            command.setUser(database.getUser(userName));
            String newName = readUniqueIdentifier();
            command.setNewName(newName);
            return command;
        } else if (readIf("ADMIN")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.ADMIN);
            User user = database.getUser(userName);
            command.setUser(user);
            if (readIf("TRUE")) {
                command.setAdmin(true);
            } else if (readIf("FALSE")) {
                command.setAdmin(false);
            } else {
                throw getSyntaxError();
            }
            return command;
        }
        throw getSyntaxError();
    }

    private void readIfEqualOrTo() throws SQLException {
        if (!readIf("=")) {
            readIf("TO");
        }
    }

    private Prepared parseSet() throws SQLException {
        if (readIf("@")) {
            Set command = new Set(session, SetTypes.VARIABLE);
            command.setString(readAliasIdentifier());
            readIfEqualOrTo();
            command.setExpression(readExpression());
            return command;
        } else if (readIf("AUTOCOMMIT")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            int setting = value ? TransactionCommand.AUTOCOMMIT_TRUE : TransactionCommand.AUTOCOMMIT_FALSE;
            return new TransactionCommand(session, setting);
        } else if (readIf("MVCC")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            Set command = new Set(session, SetTypes.MVCC);
            command.setInt(value ? 1 : 0);
            return command;
        } else if (readIf("EXCLUSIVE")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            Set command = new Set(session, SetTypes.EXCLUSIVE);
            command.setInt(value ? 1 : 0);
            return command;
        } else if (readIf("IGNORECASE")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            Set command = new Set(session, SetTypes.IGNORECASE);
            command.setInt(value ? 1 : 0);
            return command;
        } else if (readIf("PASSWORD")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setPassword(readExpression());
            return command;
        } else if (readIf("SALT")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
            return command;
        } else if (readIf("MODE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.MODE);
            command.setString(readAliasIdentifier());
            return command;
        } else if (readIf("COMPRESS_LOB")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.COMPRESS_LOB);
            if (currentTokenType == VALUE) {
                command.setString(readString());
            } else {
                command.setString(readUniqueIdentifier());
            }
            return command;
        } else if (readIf("DATABASE")) {
            readIfEqualOrTo();
            read("COLLATION");
            return parseSetCollation();
        } else if (readIf("COLLATION")) {
            readIfEqualOrTo();
            return parseSetCollation();
        } else if (readIf("CLUSTER")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.CLUSTER);
            command.setString(readString());
            return command;
        } else if (readIf("DATABASE_EVENT_LISTENER")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DATABASE_EVENT_LISTENER);
            command.setString(readString());
            return command;
        } else if (readIf("ALLOW_LITERALS")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.ALLOW_LITERALS);
            if (readIf("NONE")) {
                command.setInt(Constants.ALLOW_LITERALS_NONE);
            } else if (readIf("ALL")) {
                command.setInt(Constants.ALLOW_LITERALS_ALL);
            } else if (readIf("NUMBERS")) {
                command.setInt(Constants.ALLOW_LITERALS_NUMBERS);
            } else {
                command.setInt(getPositiveInt());
            }
            return command;
        } else if (readIf("DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DEFAULT_TABLE_TYPE);
            if (readIf("MEMORY")) {
                command.setInt(Table.TYPE_MEMORY);
            } else if (readIf("CACHED")) {
                command.setInt(Table.TYPE_CACHED);
            } else {
                command.setInt(getPositiveInt());
            }
            return command;
        } else if (readIf("CREATE")) {
            readIfEqualOrTo();
            // Derby compatibility (CREATE=TRUE in the database URL)
            read();
            return new NoOperation(session);
        } else if (readIf("HSQLDB.DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("PAGE_STORE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("CACHE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("FILE_LOCK")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("DB_CLOSE_ON_EXIT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("ACCESS_MODE_LOG")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("AUTO_SERVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("AUTO_RECONNECT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("ASSERT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("ACCESS_MODE_DATA")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("DATABASE_EVENT_LISTENER_OBJECT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("OPEN_NEW")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("RECOVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("SCHEMA")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA);
            command.setString(readAliasIdentifier());
            return command;
        } else if (readIf("DATESTYLE")) {
            // PostgreSQL compatibility
            readIfEqualOrTo();
            if (!readIf("ISO")) {
                String s = readString();
                if (!s.equals("ISO")) {
                    throw getSyntaxError();
                }
            }
            return new NoOperation(session);
        } else if (readIf("SEARCH_PATH") || readIf(SetTypes.getTypeName(SetTypes.SCHEMA_SEARCH_PATH))) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA_SEARCH_PATH);
            ObjectArray<String> list = ObjectArray.newInstance();
            list.add(readAliasIdentifier());
            while (readIf(",")) {
                list.add(readAliasIdentifier());
            }
            String[] schemaNames = new String[list.size()];
            list.toArray(schemaNames);
            command.setStringArray(schemaNames);
            return command;
        } else {
            if (isToken("LOGSIZE")) {
                // HSQLDB compatibility
                currentToken = SetTypes.getTypeName(SetTypes.MAX_LOG_SIZE);
            }
            int type = SetTypes.getType(currentToken);
            if (type < 0) {
                throw getSyntaxError();
            }
            read();
            readIfEqualOrTo();
            Set command = new Set(session, type);
            command.setExpression(readExpression());
            return command;
        }
    }

    private Set parseSetCollation() throws SQLException {
        Set command = new Set(session, SetTypes.COLLATION);
        String name = readAliasIdentifier();
        command.setString(name);
        if (name.equals(CompareMode.OFF)) {
            return command;
        }
        Collator coll = CompareMode.getCollator(name);
        if (coll == null) {
            throw getSyntaxError();
        }
        if (readIf("STRENGTH")) {
            if (readIf("PRIMARY")) {
                command.setInt(Collator.PRIMARY);
            } else if (readIf("SECONDARY")) {
                command.setInt(Collator.SECONDARY);
            } else if (readIf("TERTIARY")) {
                command.setInt(Collator.TERTIARY);
            } else if (readIf("IDENTICAL")) {
                command.setInt(Collator.IDENTICAL);
            }
        } else {
            command.setInt(coll.getStrength());
        }
        return command;
    }

    private RunScriptCommand parseRunScript() throws SQLException {
        RunScriptCommand command = new RunScriptCommand(session);
        read("FROM");
        command.setFileNameExpr(readExpression());
        if (readIf("COMPRESSION")) {
            command.setCompressionAlgorithm(readUniqueIdentifier());
        }
        if (readIf("CIPHER")) {
            command.setCipher(readUniqueIdentifier());
            if (readIf("PASSWORD")) {
                command.setPassword(readString().toCharArray());
            }
        }
        if (readIf("CHARSET")) {
            command.setCharset(readString());
        }
        return command;
    }

    private ScriptCommand parseScript() throws SQLException {
        ScriptCommand command = new ScriptCommand(session);
        boolean data = true, passwords = true, settings = true, dropTables = false, simple = false;
        if (readIf("SIMPLE")) {
            simple = true;
        }
        if (readIf("NODATA")) {
            data = false;
        }
        if (readIf("NOPASSWORDS")) {
            passwords = false;
        }
        if (readIf("NOSETTINGS")) {
            settings = false;
        }
        if (readIf("DROP")) {
            dropTables = true;
        }
        if (readIf("BLOCKSIZE")) {
            long blockSize = readLong();
            command.setLobBlockSize(blockSize);
        }
        command.setData(data);
        command.setPasswords(passwords);
        command.setSettings(settings);
        command.setDrop(dropTables);
        command.setSimple(simple);
        if (readIf("TO")) {
            command.setFileNameExpr(readExpression());
            if (readIf("COMPRESSION")) {
                command.setCompressionAlgorithm(readUniqueIdentifier());
            }
            if (readIf("CIPHER")) {
                command.setCipher(readUniqueIdentifier());
                if (readIf("PASSWORD")) {
                    command.setPassword(readString().toCharArray());
                }
            }
        }
        return command;
    }

    private Table readTableOrView() throws SQLException {
        return readTableOrView(readIdentifierWithSchema(null));
    }

    private Table readTableOrView(String tableName) throws SQLException {
        // same algorithm than readSequence
        if (schemaName != null) {
            return getSchema().getTableOrView(session, tableName);
        }
        Table table = database.getSchema(session.getCurrentSchemaName()).findTableOrView(session, tableName);
        if (table != null) {
            return table;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        for (int i = 0; schemaNames != null && i < schemaNames.length; i++) {
            Schema s = database.getSchema(schemaNames[i]);
            table = s.findTableOrView(session, tableName);
            if (table != null) {
                return table;
            }
        }
        throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

    private Sequence findSequence(String schemaName, String sequenceName) throws SQLException {
        Sequence sequence = database.getSchema(schemaName).findSequence(sequenceName);
        if (sequence != null) {
            return sequence;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        for (int i = 0; schemaNames != null && i < schemaNames.length; i++) {
            Schema s = database.getSchema(schemaNames[i]);
            sequence = s.findSequence(sequenceName);
            if (sequence != null) {
                return sequence;
            }
        }
        return null;
    }

    private Sequence readSequence() throws SQLException {
        // same algorithm than readTableOrView
        String sequenceName = readIdentifierWithSchema(null);
        if (schemaName != null) {
            return getSchema().getSequence(sequenceName);
        }
        Sequence sequence = findSequence(session.getCurrentSchemaName(), sequenceName);
        if (sequence != null) {
            return sequence;
        }
        throw Message.getSQLException(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
    }

    private Prepared parseAlterTable() throws SQLException {
        Table table = readTableOrView();
        if (readIf("ADD")) {
            Prepared command = parseAlterTableAddConstraintIf(table.getName(), table.getSchema());
            if (command != null) {
                return command;
            }
            return parseAlterTableAddColumn(table);
        } else if (readIf("SET")) {
            read("REFERENTIAL_INTEGRITY");
            int type;
            if (readIf("TRUE")) {
                type = AlterTableSet.REFERENTIAL_INTEGRITY_TRUE;
            } else {
                read("FALSE");
                type = AlterTableSet.REFERENTIAL_INTEGRITY_FALSE;
            }
            AlterTableSet command = new AlterTableSet(session, table.getSchema(), type);
            command.setTableName(table.getName());
            if (readIf("CHECK")) {
                command.setCheckExisting(true);
            } else if (readIf("NOCHECK")) {
                command.setCheckExisting(false);
            }
            return command;
        } else if (readIf("RENAME")) {
            read("TO");
            String newName = readIdentifierWithSchema(table.getSchema().getName());
            checkSchema(table.getSchema());
            AlterTableRename command = new AlterTableRename(session, getSchema());
            command.setOldTable(table);
            command.setNewTableName(newName);
            return command;
        } else if (readIf("DROP")) {
            if (readIf("CONSTRAINT")) {
                boolean ifExists = readIfExists(false);
                String constraintName = readIdentifierWithSchema(table.getSchema().getName());
                ifExists = readIfExists(ifExists);
                checkSchema(table.getSchema());
                AlterTableDropConstraint command = new AlterTableDropConstraint(session, getSchema(), ifExists);
                command.setConstraintName(constraintName);
                return command;
            } else if (readIf("PRIMARY")) {
                read("KEY");
                Index idx = table.getPrimaryKey();
                DropIndex command = new DropIndex(session, table.getSchema());
                command.setIndexName(idx.getName());
                return command;
            } else {
                readIf("COLUMN");
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema());
                command.setType(AlterTableAlterColumn.DROP);
                String columnName = readColumnIdentifier();
                command.setTable(table);
                command.setOldColumn(table.getColumn(columnName));
                return command;
            }
        } else if (readIf("ALTER")) {
            readIf("COLUMN");
            String columnName = readColumnIdentifier();
            Column column = table.getColumn(columnName);
            if (readIf("RENAME")) {
                read("TO");
                AlterTableRenameColumn command = new AlterTableRenameColumn(session);
                command.setTable(table);
                command.setColumn(column);
                String newName = readColumnIdentifier();
                command.setNewColumnName(newName);
                return command;
            } else if (readIf("SET")) {
                if (readIf("DATA")) {
                    // Derby compatibility
                    read("TYPE");
                    Column newColumn = parseColumnForTable(columnName);
                    AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema());
                    command.setTable(table);
                    command.setType(AlterTableAlterColumn.CHANGE_TYPE);
                    command.setOldColumn(column);
                    command.setNewColumn(newColumn);
                    return command;
                }
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema());
                command.setTable(table);
                command.setOldColumn(column);
                if (readIf("NULL")) {
                    command.setType(AlterTableAlterColumn.NULL);
                    return command;
                } else if (readIf("NOT")) {
                    read("NULL");
                    command.setType(AlterTableAlterColumn.NOT_NULL);
                    return command;
                } else if (readIf("DEFAULT")) {
                    Expression defaultExpression = readExpression();
                    command.setType(AlterTableAlterColumn.DEFAULT);
                    command.setDefaultExpression(defaultExpression);
                    return command;
                }
            } else if (readIf("RESTART")) {
                readIf("WITH");
                Expression start = readExpression();
                AlterSequence command = new AlterSequence(session, table.getSchema());
                command.setColumn(column);
                command.setStartWith(start);
                return command;
            } else if (readIf("SELECTIVITY")) {
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema());
                command.setTable(table);
                command.setType(AlterTableAlterColumn.SELECTIVITY);
                command.setOldColumn(column);
                command.setSelectivity(readExpression());
                return command;
            } else {
                Column newColumn = parseColumnForTable(columnName);
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema());
                command.setTable(table);
                command.setType(AlterTableAlterColumn.CHANGE_TYPE);
                command.setOldColumn(column);
                command.setNewColumn(newColumn);
                return command;
            }
        }
        throw getSyntaxError();
    }

    private AlterTableAlterColumn parseAlterTableAddColumn(Table table) throws SQLException {
        readIf("COLUMN");
        Schema schema = table.getSchema();
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        command.setType(AlterTableAlterColumn.ADD);
        command.setTable(table);
        String columnName = readColumnIdentifier();
        Column column = parseColumnForTable(columnName);
        command.setNewColumn(column);
        if (readIf("BEFORE")) {
            command.setAddBefore(readColumnIdentifier());
        }
        return command;
    }

    private int parseAction() throws SQLException {
        if (readIf("CASCADE")) {
            return ConstraintReferential.CASCADE;
        } else if (readIf("RESTRICT")) {
            return ConstraintReferential.RESTRICT;
        } else if (readIf("NO")) {
            read("ACTION");
            return ConstraintReferential.RESTRICT;
        } else {
            read("SET");
            if (readIf("NULL")) {
                return ConstraintReferential.SET_NULL;
            }
            read("DEFAULT");
            return ConstraintReferential.SET_DEFAULT;
        }
    }

    private Prepared parseAlterTableAddConstraintIf(String tableName, Schema schema) throws SQLException {
        String constraintName = null, comment = null;
        boolean ifNotExists = false;
        if (readIf("CONSTRAINT")) {
            ifNotExists = readIfNoExists();
            constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            comment = readCommentIf();
        }
        if (readIf("PRIMARY")) {
            read("KEY");
            AlterTableAddConstraint command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(AlterTableAddConstraint.PRIMARY_KEY);
            command.setComment(comment);
            command.setConstraintName(constraintName);
            command.setTableName(tableName);
            if (readIf("HASH")) {
                command.setPrimaryKeyHash(true);
            }
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            return command;
        } else if (database.getMode().indexDefinitionInCreateTable && (readIf("INDEX") || readIf("KEY"))) {
            // MySQL
            CreateIndex command = new CreateIndex(session, schema);
            command.setComment(comment);
            command.setTableName(tableName);
            if (!readIf("(")) {
                command.setIndexName(readUniqueIdentifier());
                read("(");
            }
            command.setIndexColumns(parseIndexColumnList());
            return command;
        }
        AlterTableAddConstraint command;
        if (readIf("CHECK")) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(AlterTableAddConstraint.CHECK);
            command.setCheckExpression(readExpression());
        } else if (readIf("UNIQUE")) {
            readIf("KEY");
            readIf("INDEX");
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(AlterTableAddConstraint.UNIQUE);
            if (!readIf("(")) {
                constraintName = readUniqueIdentifier();
                read("(");
            }
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
        } else if (readIf("FOREIGN")) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(AlterTableAddConstraint.REFERENTIAL);
            read("KEY");
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(schema.findIndex(session, indexName));
            }
            read("REFERENCES");
            parseReferences(command, schema, tableName);
        } else {
            if (constraintName != null) {
                throw getSyntaxError();
            }
            return null;
        }
        if (readIf("NOCHECK")) {
            command.setCheckExisting(false);
        } else {
            readIf("CHECK");
            command.setCheckExisting(true);
        }
        command.setTableName(tableName);
        command.setConstraintName(constraintName);
        command.setComment(comment);
        return command;
    }

    private void parseReferences(AlterTableAddConstraint command, Schema schema, String tableName) throws SQLException {
        if (readIf("(")) {
            command.setRefTableName(schema, tableName);
            command.setRefIndexColumns(parseIndexColumnList());
        } else {
            String refTableName = readIdentifierWithSchema(schema.getName());
            command.setRefTableName(getSchema(), refTableName);
            if (readIf("(")) {
                command.setRefIndexColumns(parseIndexColumnList());
            }
        }
        if (readIf("INDEX")) {
            String indexName = readIdentifierWithSchema();
            command.setRefIndex(getSchema().findIndex(session, indexName));
        }
        while (readIf("ON")) {
            if (readIf("DELETE")) {
                command.setDeleteAction(parseAction());
            } else {
                read("UPDATE");
                command.setUpdateAction(parseAction());
            }
        }
        if (readIf("NOT")) {
            read("DEFERRABLE");
        } else {
            readIf("DEFERRABLE");
        }
    }

    private CreateLinkedTable parseCreateLinkedTable(boolean temp, boolean globalTemp, boolean force) throws SQLException {
        read("TABLE");
        boolean ifNotExists = readIfNoExists();
        String tableName = readIdentifierWithSchema();
        CreateLinkedTable command = new CreateLinkedTable(session, getSchema());
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setForce(force);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        read("(");
        command.setDriver(readString());
        read(",");
        command.setUrl(readString());
        read(",");
        command.setUser(readString());
        read(",");
        command.setPassword(readString());
        read(",");
        String originalTable = readString();
        if (readIf(",")) {
            command.setOriginalSchema(originalTable);
            originalTable = readString();
        }
        command.setOriginalTable(originalTable);
        read(")");
        if (readIf("EMIT")) {
            read("UPDATES");
            command.setEmitUpdates(true);
        } else if (readIf("READONLY")) {
            command.setReadOnly(true);
        }
        return command;
    }

    private CreateTable parseCreateTable(boolean temp, boolean globalTemp, boolean persistIndexes) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String tableName = readIdentifierWithSchema();
        if (temp && globalTemp && "SESSION".equals(schemaName)) {
            // support weird syntax: declare global temporary table session.xy
            // (...) not logged
            schemaName = session.getCurrentSchemaName();
            globalTemp = false;
        }
        Schema schema = getSchema();
        CreateTable command = new CreateTable(session, schema);
        command.setPersistIndexes(persistIndexes);
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        if (readIf("AS")) {
            command.setQuery(parseSelect());
        } else {
            read("(");
            if (!readIf(")")) {
                do {
                    Prepared c = parseAlterTableAddConstraintIf(tableName, schema);
                    if (c != null) {
                        command.addConstraintCommand(c);
                    } else {
                        String columnName = readColumnIdentifier();
                        Column column = parseColumnForTable(columnName);
                        if (column.isAutoIncrement() && column.isPrimaryKey()) {
                            column.setPrimaryKey(false);
                            IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = column.getName();
                            AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false);
                            pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                            pk.setTableName(tableName);
                            pk.setIndexColumns(cols);
                            command.addConstraintCommand(pk);
                        }
                        command.addColumn(column);
                        String constraintName = null;
                        if (readIf("CONSTRAINT")) {
                            constraintName = readColumnIdentifier();
                        }
                        if (readIf("PRIMARY")) {
                            read("KEY");
                            boolean hash = readIf("HASH");
                            IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = column.getName();
                            AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false);
                            pk.setPrimaryKeyHash(hash);
                            pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                            pk.setTableName(tableName);
                            pk.setIndexColumns(cols);
                            command.addConstraintCommand(pk);
                            if (readIf("AUTO_INCREMENT")) {
                                parseAutoIncrement(column);
                            }
                        } else if (readIf("UNIQUE")) {
                            AlterTableAddConstraint unique = new AlterTableAddConstraint(session, schema, false);
                            unique.setConstraintName(constraintName);
                            unique.setType(AlterTableAddConstraint.UNIQUE);
                            IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = columnName;
                            unique.setIndexColumns(cols);
                            unique.setTableName(tableName);
                            command.addConstraintCommand(unique);
                        }
                        if (readIf("NOT")) {
                            read("NULL");
                            column.setNullable(false);
                        } else {
                            readIf("NULL");
                        }
                        if (readIf("CHECK")) {
                            Expression expr = readExpression();
                            column.addCheckConstraint(session, expr);
                        }
                        if (readIf("REFERENCES")) {
                            AlterTableAddConstraint ref = new AlterTableAddConstraint(session, schema, false);
                            ref.setConstraintName(constraintName);
                            ref.setType(AlterTableAddConstraint.REFERENTIAL);
                            IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = columnName;
                            ref.setIndexColumns(cols);
                            ref.setTableName(tableName);
                            parseReferences(ref, schema, tableName);
                            command.addConstraintCommand(ref);
                        }
                    }
                } while (readIfMore());
            }
            if (readIf("AS")) {
                command.setQuery(parseSelect());
            }
        }
        if (temp) {
            if (readIf("ON")) {
                read("COMMIT");
                if (readIf("DROP")) {
                    command.setOnCommitDrop();
                } else if (readIf("DELETE")) {
                    read("ROWS");
                    command.setOnCommitTruncate();
                }
            } else if (readIf("NOT")) {
                if (readIf("PERSISTENT")) {
                    command.setPersistData(false);
                } else {
                    read("LOGGED");
                }
            }
        } else if (!persistIndexes && readIf("NOT")) {
            read("PERSISTENT");
            command.setPersistData(false);
        }
        if (readIf("CLUSTERED")) {
            command.setClustered(true);
        }
        return command;
    }

    private int getCompareType(int tokenType) {
        switch (tokenType) {
        case EQUAL:
            return Comparison.EQUAL;
        case BIGGER_EQUAL:
            return Comparison.BIGGER_EQUAL;
        case BIGGER:
            return Comparison.BIGGER;
        case SMALLER:
            return Comparison.SMALLER;
        case SMALLER_EQUAL:
            return Comparison.SMALLER_EQUAL;
        case NOT_EQUAL:
            return Comparison.NOT_EQUAL;
        default:
            return -1;
        }
    }

    /**
     * Add double quotes around an identifier if required.
     *
     * @param s the identifier
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String s) {
        if (s == null || s.length() == 0) {
            return "\"\"";
        }
        char c = s.charAt(0);
        // lowercase a-z is quoted as well
        if ((!Character.isLetter(c) && c != '_') || Character.isLowerCase(c)) {
            return StringUtils.quoteIdentifier(s);
        }
        for (int i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            if ((!Character.isLetterOrDigit(c) && c != '_') || Character.isLowerCase(c)) {
                return StringUtils.quoteIdentifier(s);
            }
        }
        if (Parser.isKeyword(s, true)) {
            return StringUtils.quoteIdentifier(s);
        }
        return s;
    }

    public void setRightsChecked(boolean rightsChecked) {
        this.rightsChecked = rightsChecked;
    }

    /**
     * Parse a SQL code snippet that represents an expression.
     *
     * @param sql the code snippet
     * @return the expression object
     * @throws SQLException if the code snippet could not be parsed
     */
    public Expression parseExpression(String sql) throws SQLException {
        parameters = ObjectArray.newInstance();
        initialize(sql);
        read();
        return readExpression();
    }

}
