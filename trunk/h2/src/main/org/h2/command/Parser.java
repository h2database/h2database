/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.Collator;

import org.h2.command.ddl.AlterIndexRename;
import org.h2.command.ddl.AlterSequence;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.AlterTableDropConstraint;
import org.h2.command.ddl.AlterTableRename;
import org.h2.command.ddl.AlterTableRenameColumn;
import org.h2.command.ddl.AlterUser;
import org.h2.command.ddl.AlterView;
import org.h2.command.ddl.Analyze;
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
import org.h2.command.ddl.SetComment;
import org.h2.command.ddl.TruncateTable;
import org.h2.command.dml.Call;
import org.h2.command.dml.Delete;
import org.h2.command.dml.ExplainPlan;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.NoOperation;
import org.h2.command.dml.Query;
import org.h2.command.dml.RunScript;
import org.h2.command.dml.Script;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.command.dml.SelectUnion;
import org.h2.command.dml.Set;
import org.h2.command.dml.SetTypes;
import org.h2.command.dml.TransactionCommand;
import org.h2.command.dml.Update;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Mode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
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
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.Rownum;
import org.h2.expression.SequenceValue;
import org.h2.expression.Subquery;
import org.h2.expression.ValueExpression;
import org.h2.expression.Wildcard;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.util.ByteUtils;
import org.h2.util.ObjectArray;
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

public class Parser {

    // used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8;

    // this are token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, PARAMETER = 3, END = 4, VALUE = 5;
    private static final int EQUAL = 6, BIGGER_EQUAL = 7, BIGGER = 8;
    private static final int SMALLER = 9, SMALLER_EQUAL = 10, NOT_EQUAL = 11;
    private static final int MINUS = 17, PLUS = 18;
    private static final int STRINGCONCAT = 22;
    private static final int OPEN = 31, CLOSE = 32, NULL = 34, TRUE = 40, FALSE = 41;

    private static final int CURRENT_TIMESTAMP = 42, CURRENT_DATE = 43, CURRENT_TIME = 44, ROWNUM = 45;

    private int[] characterTypes;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private Value currentValue;
    private String sqlCommand, originalSQL;
    private char[] sqlCommandChars;
    private int lastParseIndex;
    private int parseIndex;
    private Prepared currentPrepared;
    private Select currentSelect;
    private Session session;
    private Database database;
    private ObjectArray parameters;
    private String schemaName;
    private ObjectArray expected;
    private boolean rightsChecked;
    private boolean recompileAlways;
    private ObjectArray indexedParameterList;

    public Parser(Session session) {
        this.session = session;
        database = session.getDatabase();
    }

    public Prepared prepare(String sql) throws SQLException {
        try {
            Prepared p = parse(sql);
            p.prepare();
            return p;
        } catch(Exception e) {
            throw Message.convert(e);
        }
    }

    public Prepared parseOnly(String sql) throws SQLException {
        try {
            Prepared p = parse(sql);
            return p;
        } catch(Exception e) {
            throw Message.convert(e);
        }
    }

    public Command prepareCommand(String sql) throws SQLException {
        try {
            Prepared p = parse(sql);
            p.prepare();
            Command c = new CommandContainer(this, p);
            p.setCommand(c);
            if (isToken(";")) {
                String remaining = originalSQL.substring(parseIndex);
                if(remaining.trim().length()!=0) {
                    CommandList list = new CommandList(this, c, remaining);
        //            list.addCommand(c);
        //            do {
        //                c = parseCommand();
        //                list.addCommand(c);
        //            } while(currentToken.equals(";"));
                    c = list;
                }
            } else if (currentTokenType != END) {
                // TODO exception: expected end of command
                throw getSyntaxError();
            }
            return c;
        } catch(Exception e) {
            throw Message.addSQL(Message.convert(e), this.originalSQL);
        }
    }

    private Prepared parse(String sql) throws SQLException {
        Prepared p;
        try {
            // first, try the fast variant
            p = parse(sql, false);
        } catch(SQLException e) {
            if(e.getErrorCode() == Message.SYNTAX_ERROR_1) {
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
        if(withExpectedList) {
            expected = new ObjectArray();
        } else {
            expected = null;
        }
        parameters = new ObjectArray();
        int start = lastParseIndex;
        currentSelect = null;
        currentPrepared = null;
        Prepared c = null;
        recompileAlways = false;
        indexedParameterList = null;
        read();
         String token = currentToken;
        if(token.length()==0) {
            c = new NoOperation(session);
        } else {
            char first = token.charAt(0);
            switch (first) {
            case '(':
                c = parseSelect();
                break;
            case 'A':
                if(readIf("ALTER")) {
                    c = parseAlter();
                } else if(readIf("ANALYZE")) {
                    c = parseAnalyse();
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
                }
                break;
            case 'E':
                if (readIf("EXPLAIN")) {
                    c = parseExplain();
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
                if(readIf("HELP")) {
                    c = parseHelp();
                }
                break;
            case 'I':
                if(readIf("INSERT")) {
                    c = parseInsert();
                }
                break;
            case 'M':
                if(readIf("MERGE")) {
                    c = parseMerge();
                }
                break;
            case 'P':
                if(readIf("PREPARE")) {
                    c = parsePrepareCommit();
                }
                break;
            case 'R':
                if(readIf("ROLLBACK")) {
                    c = parseRollback();
                } else if (readIf("REVOKE")) {
                    c = parseGrantRevoke(GrantRevoke.REVOKE);
                } else if(readIf("RUNSCRIPT")) {
                    c = parseRunScript();
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
                }
                break;
            case 'T':
                if(readIf("TRUNCATE")) {
                    c = parseTruncate();
                }
                break;
            case 'U':
                if(readIf("UPDATE")) {
                    c = parseUpdate();
                }
                break;
            case 'V':
                if(readIf("VALUES")) {
                    c = parserCall();
                }
                break;
            default:
                // TODO exception: unknown command
                throw getSyntaxError();
            }
            if(indexedParameterList != null) {
                for(int i=0; i<indexedParameterList.size(); i++) {
                    if(indexedParameterList.get(i) == null) {
                         indexedParameterList.set(i, new Parameter(i));
                    }
                }
                parameters = indexedParameterList;
            }
            if(readIf("{")) {
                do {
                    int index = (int)readLong() - 1;
                    if(index < 0 || index >= parameters.size()) {
                        throw getSyntaxError();
                    }
                    Parameter p = (Parameter)parameters.get(index);
                    if(p == null) {
                        throw getSyntaxError();
                    }
                    read(":");
                    Expression expr = readExpression();
                    expr = expr.optimize(session);
                    p.setValue(expr.getValue(session));
                    index++;
                } while(readIf(","));
                read("}");
                int len = parameters.size();
                for(int i=0; i<len; i++) {
                    Parameter p = (Parameter)parameters.get(i);
                    p.checkSet();
                }
                parameters.clear();
            }
        }
        if(c==null) {
            //return new ParserInt(session).parse(sql);
            throw getSyntaxError();
        }
        setSQL(c, null, start);
        return c;
    }

    private SQLException getSyntaxError() {
        if(expected == null || expected.size()==0) {
            return Message.getSyntaxError(sqlCommand, parseIndex);
        } else {
            StringBuffer buff = new StringBuffer();
            for(int i=0; i<expected.size(); i++) {
                if(i>0) {
                    buff.append(", ");
                }
                buff.append(expected.get(i));
            }
            return Message.getSyntaxError(sqlCommand, parseIndex, buff.toString());
        }
    }

    private Prepared parseAnalyse() throws SQLException {
        Analyze command = new Analyze(session);
        if(readIf("SAMPLE_SIZE")) {
            command.setTop(getPositiveInt());
        }
        return command;
    }

    private TransactionCommand parseCommit() throws SQLException {
        TransactionCommand command;
        if(readIf("TRANSACTION")) {
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
        if(readIf("IMMEDIATELY")) {
            type = TransactionCommand.SHUTDOWN_IMMEDIATELY;
        } else if(readIf("COMPACT")) {
        } else if(readIf("SCRIPT")) {
        }
        TransactionCommand command = new TransactionCommand(session, type);
        return command;
    }

    private TransactionCommand parseRollback() throws SQLException {
        TransactionCommand command;
        if(readIf("TRANSACTION")) {
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TRANSACTION);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        if(readIf("TO")) {
            read("SAVEPOINT");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TO_SAVEPOINT);
            command.setSavepointName(readUniqueIdentifier());
        } else {
            readIf("WORK");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK);
        }
        return command;
    }

    private TransactionCommand parsePrepareCommit() throws SQLException {
        TransactionCommand command = new TransactionCommand(session, TransactionCommand.PREPARE_COMMIT);
        read("COMMIT");
        command.setTransactionName(readUniqueIdentifier());
        return command;
    }

    private TransactionCommand parseSavepoint() throws SQLException {
        TransactionCommand command = new TransactionCommand(session, TransactionCommand.SAVEPOINT);
        command.setSavepointName(readUniqueIdentifier());
        return command;
    }

    private Schema getSchema() throws SQLException {
        if(schemaName == null) {
            return null;
        }
        Schema schema = database.findSchema(schemaName);
        if(schema == null) {
            if("SESSION".equals(schemaName)) {
                // for local temporary tables
                schema = database.getSchema(session.getCurrentSchemaName());
            } else {
                throw Message.getSQLException(Message.SCHEMA_NOT_FOUND_1, schemaName);
            }
        }
        return schema;
    }
    
    private Column readTableColumn(TableFilter filter) throws SQLException {
        String tableAlias = null;
        String columnName = readColumnIdentifier();
        if(readIf(".")) {
            tableAlias = columnName;
            columnName = readColumnIdentifier();
        }
        if (tableAlias != null && !tableAlias.equals(filter.getTableAlias())) {
            throw Message.getSQLException(Message.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
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
        if(readIf("(")) {
            ObjectArray columns = new ObjectArray();
            do {
                Column column = readTableColumn(filter);
                columns.add(column);
            } while(readIf(","));
            read(")");
            read("=");
            Expression expression = readExpression();
            for(int i=0; i<columns.size(); i++) {
                Column column = (Column) columns.get(i);
                Function f = Function.getFunction(database, "ARRAY_GET");
                f.setParameter(0, expression);
                f.setParameter(1, ValueExpression.get(ValueInt.get(i+1)));
                f.doneWithParameters();
                command.setAssignment(column, f);
            }
        } else {
            do {
                Column column = readTableColumn(filter);
                read("=");
                Expression expression = readExpression();
                command.setAssignment(column, expression);
            } while(readIf(","));
        }
        if (readIf("WHERE")) {
            Expression condition = readExpression();
            command.setCondition(condition);
        }
        setSQL(command, "UPDATE", start);
        return command;
    }
    
    private TableFilter readSimpleTableFilter() throws SQLException {
        String tableName = readIdentifierWithSchema();
        Table table = getSchema().getTableOrView(session, tableName);
        String alias = null;
        if(readIf("AS")) {
            alias = readAliasIdentifier();
        } else if(currentTokenType == IDENTIFIER) {
            if(!"SET".equals(currentToken)) {
                // SET is not a keyword (PostgreSQL supports it as a table name)
                alias = readAliasIdentifier();
            }
        }
        TableFilter filter = new TableFilter(session, table, alias, rightsChecked);
        return filter;
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

    private String[] parseColumnList(boolean ascDesc) throws SQLException {
        ObjectArray columns = new ObjectArray();
        do {
            String columnName = readColumnIdentifier();
            columns.add(columnName);
            if(readIf("ASC")) {
                // ignore
            } else {
                readIf("DESC");
            }
        } while(readIf(","));
        read(")");
        String[] cols = new String[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private Column[] parseColumnList(Table table) throws SQLException {
        ObjectArray columns = new ObjectArray();
        if(!readIf(")")) {
            do {
                Column column = table.getColumn(readColumnIdentifier());
                columns.add(column);
            } while(readIf(","));
            read(")");
        }
        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private Prepared parseHelp() throws SQLException {
        StringBuffer buff = new StringBuffer("SELECT * FROM INFORMATION_SCHEMA.HELP");
        int i=0;
        while (currentTokenType != END) {
            String s = currentToken;
            read();
            if(i==0) {
                buff.append(" WHERE ");
            } else {
                buff.append(" AND ");
            }
            i++;
            buff.append("UPPER(TOPIC) LIKE ");
            buff.append(StringUtils.quoteStringSQL("%"+s+"%"));
        }
        Prepared command = session.prepare(buff.toString());
        return command;
    }

    private Merge parseMerge() throws SQLException {
        Merge command = new Merge(session);
        currentPrepared = command;
        read("INTO");
        String tableName = readIdentifierWithSchema();
        Table table = getSchema().getTableOrView(session, tableName);
        command.setTable(table);
        if (readIf("(")) {
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if(readIf("KEY")) {
            read("(");
            Column[] keys = parseColumnList(table);
            command.setKeys(keys);
        }
        if (readIf("VALUES")) {
            do {
                ObjectArray values = new ObjectArray();
                read("(");
                if(!readIf(")")) {
                    do {
                        if(readIf("DEFAULT")) {
                            values.add(null);
                        } else {
                            values.add(readExpression());
                        }
                    } while(readIf(","));
                    read(")");
                }
                Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            } while(readIf(","));
        } else {
            command.setQuery(parseQueryWithParams());
        }
        return command;
    }

    private Insert parseInsert() throws SQLException {
        Insert command = new Insert(session);
        currentPrepared = command;
        read("INTO");
        String tableName = readIdentifierWithSchema();
        Table table = getSchema().getTableOrView(session, tableName);
        command.setTable(table);
        if (readIf("(")) {
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if(readIf("DEFAULT")) {
            read("VALUES");
            Expression[] expr = new Expression[0];
            command.addRow(expr);
        } else if (readIf("VALUES")) {
            do {
                ObjectArray values = new ObjectArray();
                read("(");
                if(!readIf(")")) {
                    do {
                        if(readIf("DEFAULT")) {
                            values.add(null);
                        } else {
                            values.add(readExpression());
                        }
                    } while(readIf(","));
                    read(")");
                }
                Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            } while(readIf(","));
        } else {
            command.setQuery(parseQueryWithParams());
        }
        return command;
    }

    private TableFilter readTableFilter() throws SQLException {
        Table table;
        Schema mainSchema = database.getSchema(Constants.SCHEMA_MAIN);
        if(readIf("(")) {
            if(isToken("SELECT") || isToken("FROM")) {
                Query query = parseQueryWithParams();
                String querySQL = query.getSQL();
                table = new TableView(mainSchema, 0, "TEMP_VIEW", querySQL, query.getParameters(), null, session);
                read(")");
            } else {
                TableFilter top = readTableFilter();
                top = readJoin(top, currentSelect);
                read(")");
                return top;
            }
        } else {
            String tableName = readIdentifierWithSchema();
            if(readIf("(")) {
                if(tableName.equals(RangeTable.NAME)) {
                    long min = readLong();
                    read(",");
                    long max = readLong();
                    read(")");
                    table = new RangeTable(mainSchema, min, max);
                } else {
                    Expression func = readFunction(tableName);
                    if(!(func instanceof FunctionCall)) {
                        throw getSyntaxError();
                    }
                    table = new FunctionTable(mainSchema, session, (FunctionCall)func);
                }
            } else if(tableName.equals("DUAL")) {
                table = new RangeTable(mainSchema, 1, 1);
            } else {
                table = getSchema().getTableOrView(session, tableName);
            }
        }
        String alias = null;
        if(readIf("AS")) {
            alias = readAliasIdentifier();
        } else if(currentTokenType == IDENTIFIER) {
            // left and right are not keywords (because they are functions as well)
            if(!isToken("LEFT") && !isToken("RIGHT")) {
                alias = readAliasIdentifier();
            }
        }
        TableFilter filter = new TableFilter(session, table, alias, rightsChecked);
        return filter;
    }

    private Prepared parseTruncate() throws SQLException {
        read("TABLE");
        String tableName = readIdentifierWithSchema();
        TruncateTable command = new TruncateTable(session, getSchema());
        command.setTableName(tableName);
        return command;
    }

    private boolean readIfExists(boolean ifExists) throws SQLException {
        if(readIf("IF")) {
            read("EXISTS");
            ifExists = true;
        }
        return ifExists;
    }

    private Prepared parseComment() throws SQLException {
        int type = 0;
        read("ON");
        boolean column = false;
        if(readIf("TABLE") || readIf("VIEW")) {
            type = DbObject.TABLE_OR_VIEW;
        } else if(readIf("COLUMN")) {
            column = true;
            type = DbObject.TABLE_OR_VIEW;
        } else if(readIf("CONSTANT")) {
            type = DbObject.CONSTANT;
        } else if(readIf("CONSTRAINT")) {
            type = DbObject.CONSTRAINT;
        } else if(readIf("ALIAS")) {
            type = DbObject.FUNCTION_ALIAS;
        } else if(readIf("INDEX")) {
            type = DbObject.INDEX;
        } else if(readIf("ROLE")) {
            type = DbObject.ROLE;
        } else if(readIf("SCHEMA")) {
            type = DbObject.SCHEMA;
        } else if(readIf("SEQUENCE")) {
            type = DbObject.SEQUENCE;
        } else if(readIf("TRIGGER")) {
            type = DbObject.TRIGGER;
        } else if(readIf("USER")) {
            type = DbObject.USER;
        } else if(readIf("DOMAIN")) {
            type = DbObject.USER_DATATYPE;
        } else {
            throw getSyntaxError();
        }
        SetComment command = new SetComment(session);
        String objectName = readIdentifierWithSchema();
        if(column) {
            String columnName = objectName;
            objectName = schemaName;
            schemaName = session.getCurrentSchemaName();
            if(readIf(".")) {
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
            while(readIf(",")) {
                tableName = readIdentifierWithSchema();
                DropTable next = new DropTable(session, getSchema());
                next.setTableName(tableName);
                command.addNextDropTable(next);
            }
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            if(readIf("CASCADE")) {
                readIf("CONSTRAINTS");
            }
            return command;
        } else if (readIf("INDEX")) {
            boolean ifExists = readIfExists(false);
            String indexName = readIdentifierWithSchema();
            // TODO drop index: how to drop a primary key?
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
        } else if(readIf("SEQUENCE")) {
            boolean ifExists = readIfExists(false);
            String sequenceName = readIdentifierWithSchema();
            DropSequence command = new DropSequence(session, getSchema());
            command.setSequenceName(sequenceName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("CONSTANT")) {
            boolean ifExists = readIfExists(false);
            String constantName = readIdentifierWithSchema();
            DropConstant command = new DropConstant(session, getSchema());
            command.setConstantName(constantName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("TRIGGER")) {
            boolean ifExists = readIfExists(false);
            String triggerName = readIdentifierWithSchema();
            DropTrigger command = new DropTrigger(session, getSchema());
            command.setTriggerName(triggerName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("VIEW")) {
            boolean ifExists = readIfExists(false);
            String viewName = readIdentifierWithSchema();
            DropView command = new DropView(session, getSchema());
            command.setViewName(viewName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("ROLE")) {
            boolean ifExists = readIfExists(false);
            DropRole command = new DropRole(session);
            command.setRoleName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
            // TODO role: support role names SELECT | DELETE | INSERT | UPDATE | ALL? does quoting work?
        } else if(readIf("ALIAS")) {
            boolean ifExists = readIfExists(false);
            DropFunctionAlias command = new DropFunctionAlias(session);
            command.setAliasName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("SCHEMA")) {
            boolean ifExists = readIfExists(false);
            DropSchema command = new DropSchema(session);
            command.setSchemaName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if(readIf("ALL")) {
            read("OBJECTS");
            DropDatabase command = new DropDatabase(session);
            command.setDropAllObjects(true);
            if(readIf("DELETE")) {
                read("FILES");
                command.setDeleteFiles(true);
            }
            return command;
        } else if(readIf("DOMAIN")) {
            return parseDropUserDataType();
        } else if(readIf("TYPE")) {
            return parseDropUserDataType();
        } else if(readIf("DATATYPE")) {
            return parseDropUserDataType();
        }
        throw getSyntaxError();
    }

    DropUserDataType parseDropUserDataType() throws SQLException {
        boolean ifExists = readIfExists(false);
        DropUserDataType command = new DropUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private TableFilter readJoin(TableFilter top, Select command) throws SQLException {
        TableFilter last = top;
        while (true) {
            if (readIf("RIGHT")) {
                readIf("OUTER");
                read("JOIN");
                TableFilter newTop = readTableFilter();
                Expression on = null;
                if(readIf("ON")) {
                    on = readExpression();
                }
                newTop.addJoin(top, true, on);
                top = newTop;
                last = newTop;
            } else if (readIf("LEFT")) {
                readIf("OUTER");
                read("JOIN");
                TableFilter join = readTableFilter();
                Expression on = null;
                if(readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, true, on);
                last = join;
            } else if (readIf("INNER")) {
                read("JOIN");

                TableFilter join = readTableFilter();
                Expression on = null;
                if(readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, false, on);
                last = join;
            } else if(readIf("JOIN")) {
                TableFilter join = readTableFilter();
                Expression on = null;
                if(readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, false, on);
                last = join;
            } else if(readIf("CROSS")) {
                read("JOIN");
                TableFilter join = readTableFilter();
                top.addJoin(join, false, null);
                last = join;
            } else if(readIf("NATURAL")) {
                read("JOIN");
                TableFilter join = readTableFilter();
                Column[] tc = last.getTable().getColumns();
                Column[] jc = join.getTable().getColumns();
                String ts = last.getTable().getSchema().getName();
                String js = join.getTable().getSchema().getName();
                Expression on = null;
                for(int t=0; t<tc.length; t++) {
                    String tcn = tc[t].getName();
                    for(int j=0; j<jc.length; j++) {
                        String jcn = jc[j].getName();
                        if(tcn.equals(jcn)) {
                            Expression te = new ExpressionColumn(database, currentSelect, ts, last.getTableAlias(), tcn);
                            Expression je = new ExpressionColumn(database, currentSelect, js, join.getTableAlias(), jcn);
                            Expression eq = new Comparison(session, Comparison.EQUAL, te, je);
                            if(on == null) {
                                on = eq;
                            } else {
                                on = new ConditionAndOr(ConditionAndOr.AND, on, eq);
                            }
                        }
                    }
                }
                top.addJoin(join, false, on);
                last = join;
            } else {
                break;
            }
        }
        return top;
    }

    private void parseJoinTableFilter(TableFilter top, Select command) throws SQLException {
        top = readJoin(top, command);
        command.addTableFilter(top, true);
        boolean isOuter = false;
        while(true) {
            TableFilter join = top.getJoin();
            if(join == null) {
                break;
            }
            isOuter = isOuter | join.isJoinOuter();
            if(isOuter) {
                command.addTableFilter(join, false);
            } else {
                // make flat so the optimizer can work better
                Expression on = join.getJoinCondition();
                if(on != null) {
                    command.addCondition(on);
                }
                join.removeJoinCondition();
                top.removeJoin();
                command.addTableFilter(join, true);
            }
            top = join;
        }
    }

    private ExplainPlan parseExplain() throws SQLException {
        ExplainPlan command = new ExplainPlan(session);
        readIf("PLAN");
        readIf("FOR");
        if(isToken("SELECT") || isToken("FROM")) {
            command.setCommand(parseSelect());
        } else if(isToken("(")) {
            command.setCommand(parseSelect());
        } else if(readIf("DELETE")) {
            command.setCommand(parseDelete());
        } else if(readIf("UPDATE")) {
            command.setCommand(parseUpdate());
        } else if(readIf("INSERT")) {
            command.setCommand(parseInsert());
        } else if(readIf("MERGE")) {
            command.setCommand(parseMerge());
        } else {
            throw getSyntaxError();
        }
        return command;
    }

    private Query parseSelect() throws SQLException {
        Query command = parseSelectUnion();
        command.init();
        return command;
    }
    
    private Query parseQueryWithParams() throws SQLException {
        int paramIndex = parameters.size();
        Query command = parseSelectUnion();
        command.init();
        ObjectArray params = new ObjectArray();
        for(int i=paramIndex; i<parameters.size(); i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        return command;
    }

    private Query parseSelectUnion() throws SQLException {
        int start = lastParseIndex;
        Query command = parseSelectSub();
        while(true) {
            if (readIf("UNION")) {
                SelectUnion union = new SelectUnion(session, command);
                if(readIf("ALL")) {
                    union.setUnionType(SelectUnion.UNION_ALL);
                } else {
                    readIf("DISTINCT");
                    union.setUnionType(SelectUnion.UNION);
                }
                // TODO exceptions: always add the SQL statement to the exception, if possible!
                union.setRight(parseSelectSub());
                command = union;
            } else if(readIf("MINUS") || readIf("EXCEPT")) {
                SelectUnion union = new SelectUnion(session, command);
                union.setUnionType(SelectUnion.EXCEPT);
                union.setRight(parseSelectSub());
                command = union;
            } else if(readIf("INTERSECT")) {
                SelectUnion union = new SelectUnion(session, command);
                union.setUnionType(SelectUnion.INTERSECT);
                union.setRight(parseSelectSub());
                command = union;
            } else {
                break;
            }
        }
        if (readIf("ORDER")) {
            read("BY");
            Select oldSelect = currentSelect;
            if(command instanceof Select) {
                currentSelect = (Select)command;
            }
            ObjectArray orderList = new ObjectArray();
            do {
                boolean canBeNumber = true;
                if(readIf("=")) {
                    canBeNumber = false;
                }
                SelectOrderBy order = new SelectOrderBy();
                Expression expr = readExpression();
                if(canBeNumber && expr instanceof ValueExpression && expr.getType() == Value.INT) {
                    int i = expr.getValue(null).getInt();
                    order.column = i-1;
                } else {
                    order.expression = expr;
                }
                if(readIf("DESC")) {
                    order.descending = true;
                } else {
                    readIf("ASC");
                }
                if(readIf("NULLS")) {
                    if(readIf("FIRST")) {
                        order.nullsFirst = true;
                    } else {
                        read("LAST");
                        order.nullsLast = true;
                    }
                }
                orderList.add(order);
            } while(readIf(","));
            command.setOrder(orderList);
            currentSelect = oldSelect;
        }
        if(readIf("LIMIT")) {
            Expression limit = readExpression().optimize(session);
            command.setLimit(limit);
            if(readIf("OFFSET")) {
                Expression offset = readExpression().optimize(session);
                command.setOffset(offset);
            } else if(readIf(",")) {
                // MySQL: [offset, ] rowcount
                Expression offset = limit;
                limit = readExpression().optimize(session);
                command.setOffset(offset);
                command.setLimit(limit);
            }
            if(readIf("SAMPLE_SIZE")) {
                command.setSampleSize(getPositiveInt());
            }
        }
        if(readIf("FOR")) {
            if(readIf("UPDATE")) {
                if(readIf("OF")) {
                    // TODO parser: select for update of: should do something with the list!
                    do {
                        readIdentifierWithSchema();
                    } while(readIf(","));
                } else if(readIf("NOWAIT")) {
                    // TODO parser: select for update nowait: should not wait
                } else if(readIf("WITH")) {
                    // Hibernate / Derby support
                    read("RR");
                }
                command.setForUpdate(true);
            } else if(readIf("READ")) {
                read("ONLY");
                if(readIf("WITH")) {
                    read("RS");
                }
            }
        }
        setSQL(command, null, start);
        return command;
    }

    private Query parseSelectSub() throws SQLException {
        if(readIf("(")) {
            Query command = parseSelectUnion();
            read(")");
            return command;
        }
        Select select = parseSelectSimple();
        return select;
    }

    private void parseSelectSimpleFromPart(Select command) throws SQLException {
        do {
            TableFilter filter = readTableFilter();
            parseJoinTableFilter(filter, command);
        } while(readIf(","));
    }

    private void parseSelectSimpleSelectPart(Select command) throws SQLException {
        if(readIf("TOP")) {
            // can't read more complex expressions here because
            // SELECT TOP 1 +? A FROM TEST could mean
            // SELECT TOP (1+?) A FROM TEST or
            // SELECT TOP 1 (+?) AS A FROM TEST
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        } else if(readIf("LIMIT")) {
            Expression offset = readTerm().optimize(session);
            command.setOffset(offset);
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        }
        if(readIf("DISTINCT")) {
            command.setDistinct(true);
        } else {
            readIf("ALL");
        }
        ObjectArray expressions = new ObjectArray();
        do {
            if(readIf("*")) {
                expressions.add(new Wildcard(null, null));
            } else {
                Expression expr = readExpression();
                if(readIf("AS") || currentTokenType == IDENTIFIER) {
                    String alias = readAliasIdentifier();
                    expr = new Alias(expr, alias);
                }
                expressions.add(expr);
            }
        } while(readIf(","));
        command.setExpressions(expressions);
    }

    private Select parseSelectSimple() throws SQLException {
        boolean fromFirst;
        if(readIf("SELECT")) {
            fromFirst = false;
        } else if(readIf("FROM")) {
            fromFirst = true;
        } else {
            throw getSyntaxError();
        }
        Select command = new Select(session);
        int start = lastParseIndex;
        Select oldSelect = currentSelect;
        currentSelect = command;
        currentPrepared = command;
        if(fromFirst) {
            parseSelectSimpleFromPart(command);
            read("SELECT");
            parseSelectSimpleSelectPart(command);
        } else {
            parseSelectSimpleSelectPart(command);
            if(!readIf("FROM")) {
                // select without FROM: convert to SELECT ... FROM SYSTEM_RANGE(1,1)
                Schema main = database.findSchema(Constants.SCHEMA_MAIN);
                Table dual = new RangeTable(main, 1, 1);
                TableFilter filter = new TableFilter(session, dual, null, rightsChecked);
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
            ObjectArray list = new ObjectArray();
            do {
                Expression expr = readExpression();
                list.add(expr);
            } while(readIf(","));
            command.setGroupBy(list);
        }
        currentSelect = command;
        if(readIf("HAVING")) {
            command.setGroupQuery();
            Expression condition = readExpression();
            command.setHaving(condition);
        }
        currentSelect = oldSelect;
        setSQL(command, "SELECT", start);
        return command;
    }

    private void setSQL(Prepared command, String start, int startIndex) {
        String sql = originalSQL.substring(startIndex, lastParseIndex).trim();
        if(start != null) {
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
            Query query = parseQueryWithParams();
            // can not reduce expression because it might be a union except query with distinct
            read(")");
            return new ConditionExists(query);
        }
        Expression r = readConcat();
        while(true) {
            // special case: NOT NULL is not part of an expression (as in CREATE TABLE TEST(ID INT DEFAULT 0 NOT NULL))
            int backup = parseIndex;
            boolean not = false;
            if (readIf("NOT")) {
                not = true;
                if(isToken("NULL")) {
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
                    esc = readExpression();
                }
                recompileAlways = true;
                r = new CompareLike(database.getCompareMode(), r, b, esc);
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
                // TODO extend IN to support arrays (using setArray?)
                if(Constants.OPTIMIZE_IN) {
                    recompileAlways = true;
                }
                read("(");
                if(readIf(")")) {
                    r = ValueExpression.get(ValueBoolean.get(false));
                } else {
                    if (isToken("SELECT") || isToken("FROM")) {
                        Query query = parseQueryWithParams();
                        r = new ConditionInSelect(database, r, query, false, Comparison.EQUAL);
                    } else {
                        ObjectArray v = new ObjectArray();
                        Expression last;
                        do {
                            last = readExpression();
                            v.add(last);
                        } while(readIf(","));
                        if(v.size()==1 && (last instanceof Subquery)) {
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
                // TODO parser: if we use a switch case, we don't need getCompareType any more
                int compareType = getCompareType(currentTokenType);
                if(compareType < 0) {
                    break;
                }
                read();
                if(readIf("ALL")) {
                    read("(");
                    Query query = parseQueryWithParams();
                    r = new ConditionInSelect(database, r, query, true, compareType);
                    read(")");
                } else if(readIf("ANY") || readIf("SOME")) {
                    read("(");
                    Query query = parseQueryWithParams();
                    r = new ConditionInSelect(database, r, query, false, compareType);
                    read(")");
                } else {
                    Expression right = readConcat();
                    if(readIf("(") && readIf("+") && readIf(")")) {
                        // support for a subset of old-fashioned Oracle outer join with (+)
                        if(r instanceof ExpressionColumn && right instanceof ExpressionColumn) {
                            ExpressionColumn lcol = (ExpressionColumn) r;
                            ExpressionColumn rcol = (ExpressionColumn) right;
                            ObjectArray filters = currentSelect.getTopFilters();
                            for(int i=0; filters != null && i<filters.size(); i++) {
                                TableFilter f = (TableFilter) filters.get(i);
                                lcol.mapColumns(f, 0);
                                rcol.mapColumns(f, 0);
                            }
                            TableFilter lfilter = lcol.getTableFilter();
                            TableFilter rfilter = rcol.getTableFilter();
                            r = new Comparison(session, compareType, r, right);
                            if(lfilter != null && rfilter != null) {
                                filters.remove(filters.indexOf(rfilter));
                                lfilter.addJoin(rfilter, true, r);
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
        while (readIf("||")) {
            r = new Operation(Operation.CONCAT, r, readSum());
        }
        return r;
    }

    private Expression readSum() throws SQLException {
        Expression r = readFactor();
        while(true) {
            if(readIf("+")) {
                r = new Operation(Operation.PLUS, r, readFactor());
            } else if(readIf("-")) {
                r = new Operation(Operation.MINUS, r, readFactor());
            } else {
                return r;
            }
        }
    }

    private Expression readFactor() throws SQLException {
        Expression r = readTerm();
        while(true) {
            if(readIf("*")) {
                r = new Operation(Operation.MULTIPLY, r, readTerm());
            } else if(readIf("/")) {
                r = new Operation(Operation.DIVIDE, r, readTerm());
            } else {
                return r;
            }
        }
    }

    private Expression readAggregate(int aggregateType) throws SQLException {
        if(currentSelect == null) {
            // TODO exception: function only allowed in a query
            throw getSyntaxError();
        }
        currentSelect.setGroupQuery();
        Expression r;
        if(aggregateType == Aggregate.COUNT) {
            if(readIf("*")) {
                r = new Aggregate(database, Aggregate.COUNT_ALL, null, currentSelect, false);
            } else {
                boolean distinct = readIf("DISTINCT");
                r = new Aggregate(database, Aggregate.COUNT, readExpression(), currentSelect, distinct);
            }
        } else if(aggregateType == Aggregate.GROUP_CONCAT) {
            boolean distinct = readIf("DISTINCT");
            Aggregate agg = new Aggregate(database, Aggregate.GROUP_CONCAT, readExpression(), currentSelect, distinct);
            if(readIf("ORDER")) {
                read("BY");
                agg.setOrder(parseSimpleOrderList());
            }
            if(readIf("SEPARATOR")) {
                agg.setSeparator(readExpression());
            }
            r = agg;
        } else {
            boolean distinct = readIf("DISTINCT");
            r = new Aggregate(database, aggregateType, readExpression(), currentSelect, distinct);
        }
        read(")");
        return r;
    }

    private ObjectArray parseSimpleOrderList() throws SQLException {
        ObjectArray orderList = new ObjectArray();
        do {
            SelectOrderBy order = new SelectOrderBy();
            Expression expr = readExpression();
            order.expression = expr;
            if(readIf("DESC")) {
                order.descending = true;
            } else {
                readIf("ASC");
            }
            orderList.add(order);
        } while(readIf(","));
        return orderList;
    }

    private JavaFunction readJavaFunction(String name) throws SQLException {
        FunctionAlias functionAlias = database.findFunctionAlias(name);
        if(functionAlias == null) {
            // TODO compatiblity: maybe support 'on the fly java functions' as HSQLDB ( CALL "java.lang.Math.sqrt"(2.0) )
            throw Message.getSQLException(Message.FUNCTION_NOT_FOUND_1, name);
        }
        int paramCount = functionAlias.getParameterCount();
        Expression[] args = new Expression[paramCount];
        for(int i=0; i<args.length; i++) {
            if(i>0) {
                read(",");
            }
            args[i] = readExpression();
        }
        read(")");
        JavaFunction func = new JavaFunction(functionAlias, args);
        return func;
    }

    private Expression readFunction(String name) throws SQLException {
        int agg = Aggregate.getAggregateType(name);
        if(agg >= 0) {
            return readAggregate(agg);
        }
        Function function = Function.getFunction(database, name);
        if(function==null) {
            return readJavaFunction(name);
        }
        switch(function.getFunctionType()) {
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
        case Function.SUBSTRING: {
            function.setParameter(0, readExpression());
            if(!readIf(",")) {
                read("FROM");
            }
            function.setParameter(1, readExpression());
            if(readIf("FOR") || readIf(",")) {
                function.setParameter(2, readExpression());
            }
            read(")");
            break;
        }
        case Function.POSITION: {
            // can't read expession because IN would be read too early
            function.setParameter(0, readConcat());
            if(!readIf(",")) {
                read("IN");
            }
            function.setParameter(1, readExpression());
            read(")");
            break;
        }
        case Function.TRIM: {
            Expression space = null;
            if(readIf("LEADING")) {
                function = Function.getFunction(database, "LTRIM");
                if(!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            } else if(readIf("TRAILING")) {
                function = Function.getFunction(database, "RTRIM");
                if(!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            } else if(readIf("BOTH")) {
                if(!readIf("FROM")) {
                    space = readExpression();
                    read("FROM");
                }
            }
            Expression p0 = readExpression();
            if(readIf(",")) {
                space = readExpression();
            } else if(readIf("FROM")) {
                space = p0;
                p0 = readExpression();
            }
            function.setParameter(0, p0);
            if(space != null) {
                function.setParameter(1, space);
            }
            read(")");
            break;
        }
        default:
            if(!readIf(")")) {
                int i=0;
                do {
                    function.setParameter(i++, readExpression());
                } while(readIf(","));
                read(")");
            }
        }
        function.doneWithParameters();
        return function;
    }

    private Function readFunctionWithoutParameters(String name) throws SQLException {
        if(readIf("(")) {
            read(")");
        }
        Function function = Function.getFunction(database, name);
        function.doneWithParameters();
        return function;
    }
    
    private Expression readWildcardOrSequenceValue(String schemaName, String objectName) throws SQLException {
        if(readIf("*")) {
            return new Wildcard(schemaName, objectName);
        }
        if(schemaName == null) {
            schemaName = session.getCurrentSchemaName();
        }
        if(readIf("NEXTVAL")) {
            Sequence sequence = database.getSchema(schemaName).findSequence(objectName);
            if(sequence != null) {
                return new SequenceValue(sequence);
            }
        } else if(readIf("CURRVAL")) {
            Sequence sequence = database.getSchema(schemaName).findSequence(objectName);
            if(sequence != null) {
                Function function = Function.getFunction(database, "CURRVAL");
                function.setParameter(0, ValueExpression.get(ValueString.get(objectName)));
                return function;
            }
        }
        return null;
    }

    private Expression readTermObjectDot(String objectName) throws SQLException {
        Expression expr = readWildcardOrSequenceValue(null, objectName);
        if(expr != null) {
            return expr;
        }
        String name = readColumnIdentifier();
        if(readIf(".")) {
            String schemaName = objectName;
            objectName = name;
            expr = readWildcardOrSequenceValue(schemaName, objectName);
            if(expr != null) {
                return expr;
            }
            name = readColumnIdentifier();
            return new ExpressionColumn(database, currentSelect, schemaName, objectName, name);
        }
        return new ExpressionColumn(database, currentSelect, null, objectName, name);
    }
    
    private Expression readTerm() throws SQLException {
        Expression r = null;
        switch (currentTokenType) {
        case PARAMETER:
            // there must be no space between ? and the number
            boolean indexed = Character.isDigit(sqlCommandChars[parseIndex]);
            read();
            if(indexed && currentTokenType == VALUE && currentValue.getType() == Value.INT) {
                if(indexedParameterList == null) {
                    if(parameters.size()>0) {
                        throw Message.getSQLException(Message.CANT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                    }
                    indexedParameterList = new ObjectArray();
                }
                int index = currentValue.getInt() - 1;
                if(index < 0 || index >= Constants.MAX_PARAMETER_INDEX) {
                    throw Message.getInvalidValueException("" + index, "Parameter Index");
                }
                if(indexedParameterList.size() <= index) {
                    indexedParameterList.setSize(index + 1);
                }
                r = (Parameter) indexedParameterList.get(index);
                if(r == null) {
                    r = new Parameter(index);
                    indexedParameterList.set(index, r);
                }
                read();
            } else {
                if(indexedParameterList != null) {
                    throw Message.getSQLException(Message.CANT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                }
                r = new Parameter(parameters.size());
            }
            parameters.add(r);
            break;
        case KEYWORD:
            if(isToken("SELECT") || isToken("FROM")) {
                Query query = parseQueryWithParams();
                return new Subquery(query);
            }
            throw getSyntaxError();
        case IDENTIFIER:
            String name = currentToken;
            if(currentTokenQuoted) {
                read();
                if(readIf(".")) {
                    return readTermObjectDot(name);
                }
                return new ExpressionColumn(database, currentSelect, null, null, name);
            }
            read();
            if("X".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                read();
                byte[] buffer = ByteUtils.convertStringToBytes(currentValue.getString());
                r = ValueExpression.get(ValueBytes.get(buffer));
            } else if(readIf(".")) {
                return readTermObjectDot(name);
            } else if (readIf("(")) {
                return readFunction(name);
            } else if("CURRENT".equals(name)) {
                if(readIf("TIMESTAMP")) {
                    return readFunctionWithoutParameters("CURRENT_TIMESTAMP");
                } else if(readIf("TIME")) {
                    return readFunctionWithoutParameters("CURRENT_TIME");
                } else if(readIf("DATE")) {
                    return readFunctionWithoutParameters("CURRENT_DATE");
                } else {
                    return new ExpressionColumn(database, currentSelect, null, null, name);
                }
            } else if("NEXT".equals(name) && readIf("VALUE")) {
                read("FOR");
                String sequenceName = readIdentifierWithSchema();
                Sequence sequence = getSchema().getSequence(sequenceName);
                return new SequenceValue(sequence);
            } else if("DATE".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                String date = currentValue.getString();
                read();
                return ValueExpression.get(ValueDate.get(ValueDate.parseDate(date)));
            } else if("TIME".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                String time = currentValue.getString();
                read();
                return ValueExpression.get(ValueTime.get(ValueTime.parseTime(time)));
            } else if("TIMESTAMP".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                String timestamp = currentValue.getString();
                read();
                return ValueExpression.get(ValueTimestamp.get(ValueTimestamp.parseTimestamp(timestamp)));
            } else if("CASE".equals(name)) {
                if(isToken("WHEN")) {
                    return readWhen(null);
                } else {
                    Expression left = readExpression();
                    return readWhen(left);
                }
            } else {
                return new ExpressionColumn(database, currentSelect, null, null, name);
            }
            break;
        case MINUS:
            read();
            if (currentTokenType == VALUE) {
                Expression e = ValueExpression.get(currentValue.negate());
                // convert Integer.MIN_VALUE to int (-Integer.MIN_VALUE needed to be a long)
                if(e.getType() == Value.LONG && e.getValue(session).getLong() == Integer.MIN_VALUE) {
                    e = ValueExpression.get(ValueInt.get(Integer.MIN_VALUE));
                }
                // TODO parser: maybe convert Long.MIN_VALUE from decimal to long?
                read();
                return e;
            }
            return new Operation(Operation.NEGATE, readTerm(), null);
        case PLUS:
            read();
            return readTerm();
        case OPEN:
            read();
            r = readExpression();
            if(readIf(",")) {
                ObjectArray list = new ObjectArray();
                list.add(r);
                do {
                    r = readExpression();
                    list.add(r);
                } while(readIf(","));
                Expression[] array = new Expression[list.size()];
                list.toArray(array);
                r = new ExpressionList(array);
            }
            read(")");
            break;
        case TRUE:
            read();
            return ValueExpression.get(ValueBoolean.get(true));
        case FALSE:
            read();
            return ValueExpression.get(ValueBoolean.get(false));
        case CURRENT_TIME:
            read();
            return readFunctionWithoutParameters("CURRENT_TIME");
        case CURRENT_DATE:
            read();
            return readFunctionWithoutParameters("CURRENT_DATE");
        case CURRENT_TIMESTAMP: {
            Function function = Function.getFunction(database, "CURRENT_TIMESTAMP");
            read();
            if(readIf("(")) {
                if(!readIf(")")) {
                    function.setParameter(0, readExpression());
                    read(")");
                }
            }
            function.doneWithParameters();
            return function;
        }
        case ROWNUM:
            read();
            if(readIf("(")) {
                read(")");
            }
            return new Rownum(currentSelect == null ? currentPrepared : currentSelect);
        case NULL:
            read();
            if(readIf("::")) {
                // PostgreSQL compatibility
                parseColumn(null);
            }
            return ValueExpression.NULL;
        case VALUE:
            r = ValueExpression.get(currentValue);
            read();
            break;
        default:
            // TODO exception: expected a term
            throw getSyntaxError();
        }
        return r;
    }

    private Expression readWhen(Expression left) throws SQLException {
        if(readIf("END")) {
            readIf("CASE");
            return ValueExpression.NULL;
        }
        if(readIf("ELSE")) {
            Expression elsePart = readExpression();
            read("END");
            readIf("CASE");
            return elsePart;
        }
        readIf("WHEN");
        Expression when = readExpression();
        if(left != null) {
            when = new Comparison(session, Comparison.EQUAL, left, when);
        }
        read("THEN");
        Expression then = readExpression();
        Expression elsePart = readWhen(left);
        Function function = Function.getFunction(session.getDatabase(), "CASEWHEN");
        function.setParameter(0, when);
        function.setParameter(1, then);
        function.setParameter(2, elsePart);
        return function;
    }

    private int getPositiveInt() throws SQLException {
        int v = getInt();
        if(v < 0) {
            throw Message.getInvalidValueException("" + v, "positive integer");
        }
        return v;
    }

    private int getInt() throws SQLException {
        boolean minus = false;
        if(currentTokenType == MINUS) {
            minus = true;
            read();
        } else if(currentTokenType == PLUS) {
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
        if(currentTokenType == MINUS) {
            minus = true;
            read();
        }
        if (currentTokenType != VALUE || (currentValue.getType() != Value.INT && currentValue.getType() != Value.DECIMAL)) {
            throw Message.getSyntaxError(sqlCommand, parseIndex, "long");
        }
        long i = currentValue.getLong();
        read();
        return minus ? -i : i;
    }

    private boolean readBooleanSetting() throws SQLException {
        if(currentTokenType==VALUE) {
            boolean result = currentValue.getBoolean().booleanValue();
            read();
            return result;
        }
        if(readIf("TRUE") || readIf("ON")) {
            return true;
        } else if(readIf("FALSE") || readIf("OFF")) {
            return false;
        } else {
            throw getSyntaxError();
        }
    }

    private String readString() throws SQLException {
        // TODO parser: readInt/Long could maybe use readExpression as well
        Expression expr = readExpression().optimize(session);
        if(!(expr instanceof ValueExpression)) {
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
        if(token.equals(currentToken) && !currentTokenQuoted) {
            read();
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean isToken(String token) {
        boolean result = token.equals(currentToken) && !currentTokenQuoted;
        if(result) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private void addExpected(String token) {
        if(expected != null) {
            expected.add(token);
        }
    }

    private void read() throws SQLException {
        currentTokenQuoted = false;
        if(expected != null) {
            expected.clear();
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
                    c = chars[i];
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
            while(true) {
                for(int begin=i; ; i++) {
                    if(chars[i]=='\"') {
                        if(result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin-1, i);
                        }
                        break;
                    }
                }
                if(chars[++i] != '\"') {
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
            // fall through
        case CHAR_SPECIAL_1:
            currentToken = sqlCommand.substring(start, i);
            currentTokenType = getSpecialType(currentToken);
            parseIndex = i;
            return;
        case CHAR_VALUE:
            if(c == '0' && chars[i] == 'X') {
                // hex number
                long number = 0;
                start += 2;
                i++;
                while (true) {
                    c = chars[i];
                    if ((c < '0' || c > '9') && (c<'A' || c>'F')) {
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
            while(true) {
                for(int begin=i; ; i++) {
                    if(chars[i]=='\'') {
                        if(result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin-1, i);
                        }
                        break;
                    }
                }
                if(chars[++i] != '\'') {
                    break;
                }
                i++;
            }
            currentToken = "'";
            checkLiterals(false);
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
            // TODO exception: unsupported character
            throw getSyntaxError();
        }
    }

    private void checkLiterals(boolean text) throws SQLException {
        if(!session.getAllowLiterals()) {
            int allowed = database.getAllowLiterals();
            if(allowed == Constants.ALLOW_LITERALS_NONE || (text &&  allowed != Constants.ALLOW_LITERALS_ALL)) {
                throw Message.getSQLException(Message.LITERALS_ARE_NOT_ALLOWED);
            }
        }
    }

    private void readHexDecimal(int start, int i) throws SQLException {
        char[] chars = sqlCommandChars;
        char c;
        do {
            c = chars[++i];
        } while ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) ;
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
        while(true) {
            int t = types[i];
            if(t != CHAR_DECIMAL && t != CHAR_VALUE) {
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
                // TODO exception: error reading value
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
            throw Message.getSQLException(Message.DATA_CONVERSION_ERROR_1, new String[] { sub }, e);
        }
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public Session getSession() {
        return session;
    }

    private void initialize(String sql) throws SQLException {
        if(sql == null) {
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
        // TODO optimization in parser: could remember the length of each token
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
                    break;
                } else if(command[i + 1] == '/') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len-1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                    break;
                }
                // fallthrough
            case '-':
                if (command[i + 1] == '-') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len-1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                    break;
                }
                // fallthrough
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
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '|':
            case '=':
            case ':':
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
                // SQL Server alias for "
                command[i] = '"';
                changed = true;
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != ']') {
                    checkRunOver(i, len, startLoop);
                }
                command[i] = '"';
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
                    if(Character.isLetterOrDigit(c)) {
                        type = CHAR_NAME;
                        char u = Character.toUpperCase(c);
                        if(u != c) {
                            command[i] = u;
                            changed = true;
                        }
                    }
                }
            }
            types[i] = (byte)type;
        }
        sqlCommandChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        if(changed) {
            sqlCommand = new String(command);
        }
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) throws SQLException {
        if(i >= len) {
            parseIndex = startLoop;
            // TODO exception: unexpected end
            throw getSyntaxError();
        }
    }

    private int getSpecialType(String s) throws SQLException {
        char c0 = s.charAt(0);
        if(s.length()==1) {
            switch(c0) {
            case '?':
                return PARAMETER;
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
            }
        } else if(s.length()==2) {
            switch (c0) {
            case ':':
                if(s.equals("::")) {
                    return KEYWORD;
                }
                break;
            case '>':
                if(s.equals(">=")) {
                    return BIGGER_EQUAL;
                }
                break;
            case '<':
                if (s.equals("<=")) {
                    return SMALLER_EQUAL;
                } else if (s.equals("<>")) {
                    return NOT_EQUAL;
                }
                break;
            case '!':
                if (s.equals("!=")) {
                    return NOT_EQUAL;
                }
                break;
            case '|':
                if(s.equals("||")) {
                    return STRINGCONCAT;
                }
            }
        }
        throw getSyntaxError();
    }

    private int getTokenType(String s) throws SQLException {
        // TODO the list of keywords is in the documentation! should be a hash map!
        int len = s.length();
        if(len == 0) {
            throw getSyntaxError();
        }
        return getSaveTokenType(s);
    }

    public static boolean isKeyword(String s) {
        if(s == null || s.length() == 0) {
            return false;
        }
        return getSaveTokenType(s) != IDENTIFIER;
    }

    private static int getSaveTokenType(String s) {
        switch (s.charAt(0)) {
        case 'C':
            if(s.endsWith("CURRENT_TIMESTAMP")) {
                return CURRENT_TIMESTAMP;
            } else if(s.endsWith("CURRENT_TIME")) {
                return CURRENT_TIME;
            } else if(s.endsWith("CURRENT_DATE")) {
                return CURRENT_DATE;
            }
            return getKeywordOrIdentifier(s, "CROSS", KEYWORD);
        case 'D':
            return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
        case 'E':
            if(s.equals("EXCEPT")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
        case 'F':
            if(s.equals("FROM")) {
                return KEYWORD;
            } else if(s.equals("FOR")) {
                return KEYWORD;
            } else if(s.equals("FULL")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "FALSE", FALSE);
        case 'G':
            return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
        case 'H':
            return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
        case 'I':
            if (s.equals("INNER")) {
                return KEYWORD;
            } else if(s.equals("INTERSECT")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "IS", KEYWORD);
        case 'J':
            return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
        case 'L':
            if (s.equals("LIMIT")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
        case 'M':
            if(s.equals("MINUS")) {
                return KEYWORD;
            }
            break;
        case 'N':
            if(s.equals("NOT")) {
                return KEYWORD;
            } else if(s.equals("NATURAL")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "NULL", NULL);
        case 'O':
            if (s.equals("ON")) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
        case 'P':
            return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
        case 'R':
            return getKeywordOrIdentifier(s, "ROWNUM", ROWNUM);
        case 'S':
            if(s.endsWith("SYSTIMESTAMP")) {
                return CURRENT_TIMESTAMP;
            } else if(s.endsWith("SYSTIME")) {
                return CURRENT_TIME;
            } else if(s.endsWith("SYSDATE")) {
                return CURRENT_DATE;
            }
            return getKeywordOrIdentifier(s, "SELECT", KEYWORD);
        case 'T':
            if(s.equals("TODAY")) {
                return CURRENT_DATE;
            }
            return getKeywordOrIdentifier(s, "TRUE", TRUE);
        case 'U':
            return getKeywordOrIdentifier(s, "UNION", KEYWORD);
        case 'W':
            return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
        }
        return IDENTIFIER;
    }

    private static int getKeywordOrIdentifier(String s1, String s2, int keywordType) {
        if (s1.equals(s2)) {
            return keywordType;
        }
        return IDENTIFIER;
    }

    private Column parseColumnForTable(String columnName) throws SQLException {
        Column column;
        if(readIf("IDENTITY")) {
            column = new Column(columnName, Value.LONG, ValueLong.PRECISION, 0);
            column.setOriginalSQL("IDENTITY");
            long start = 1, increment = 1;
            if(readIf("(")) {
                start = readLong();
                if(readIf(",")) {
                    increment = readLong();
                }
                read(")");
            }
            column.setAutoIncrement(true, start, increment);
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
        if(readIf("AS")) {
            Expression expr = readExpression();
            column.setComputed(true, expr);
        } else if (readIf("DEFAULT")) {
            Expression defaultExpression = readExpression();
            column.setDefaultExpression(session, defaultExpression);
        } else if(readIf("GENERATED")) {
            read("BY");
            read("DEFAULT");
            read("AS");
            read("IDENTITY");
            long start = 1, increment = 1;
            if(readIf("(")) {
                read("START");
                read("WITH");
                start = readLong();
                readIf(",");
                if(readIf("INCREMENT")) {
                    read("BY");
                    increment = readLong();
                }
                read(")");
            }
            column.setAutoIncrement(true, start, increment);
        }
        if (readIf("NOT")) {
            read("NULL");
            column.setNullable(false);
        } else {
            readIf("NULL");
        }
        if(readIf("AUTO_INCREMENT") || readIf("IDENTITY")) {
            long start = 1, increment = 1;
            if(readIf("(")) {
                start = readLong();
                if(readIf(",")) {
                    increment = readLong();
                }
                read(")");
            }
            column.setAutoIncrement(true, start, increment);
            if (readIf("NOT")) {
                read("NULL");
            }
        }
        if(readIf("NULL_TO_DEFAULT")) {
            column.setConvertNullToDefault(true);
        }
        if(readIf("SEQUENCE")) {
            String sequenceName = readIdentifierWithSchema();
            Sequence sequence = getSchema().getSequence(sequenceName);
            column.setSequence(sequence);
        }
        if(readIf("SELECTIVITY")) {
            int sel = getPositiveInt();
            column.setSelectivity(sel);
        }
        column.setComment(readCommentIf());
        return column;
    }
    
    private String readCommentIf() throws SQLException {
        if(readIf("COMMENT")) {
            readIf("IS");
            return readString();
        }
        return null;
    }

    private Column parseColumn(String columnName) throws SQLException {
        String original = currentToken;
        boolean regular = false;
        if(readIf("LONG")) {
            if(readIf("RAW")) {
                original += " RAW";
            }
        } else if(readIf("DOUBLE")) {
            if(readIf("PRECISION")) {
                original += " PRECISION";
            }
        } else {
            regular = true;
        }
        DataType dataType = DataType.getTypeByName(original);
        long precision = -1;
        int scale = -1;
        Column templateColumn = null;
        if(dataType==null) {
            UserDataType userDataType = database.findUserDataType(original);
            if(userDataType == null) {
                throw Message.getSQLException(Message.UNKNOWN_DATA_TYPE_1, currentToken);
            } else {
                templateColumn = userDataType.getColumn();
                dataType = DataType.getDataType(templateColumn.getType());
                original = templateColumn.getOriginalSQL();
                precision = templateColumn.getPrecision();
                scale = templateColumn.getScale();
            }
        }
        if(database.getIgnoreCase() && dataType.type == Value.STRING && !"VARCHAR_CASESENSITIVE".equals(original)) {
            original = "VARCHAR_IGNORECASE";
            dataType = DataType.getTypeByName(original);
        }
        if(regular) {
            read();
        }
        precision = precision == -1 ? dataType.defaultPrecision : precision;
        scale = scale == -1 ? dataType.defaultScale : scale;
        if(dataType.supportsPrecision || dataType.supportsScale) {
            if(readIf("(")) {
                precision = getPositiveInt();
                if(readIf("K")) {
                    precision *= 1024;
                } else if(readIf("M")) {
                    precision *= 1024 * 1024;
                } else if(readIf("G")) {
                    precision *= 1024 * 1024 * 1024;
                }
                if(precision > Integer.MAX_VALUE) {
                    precision = Integer.MAX_VALUE;
                }
                original += "(" + precision;
                // oracle syntax
                readIf("CHAR");
                if(dataType.supportsScale) {
                    if(readIf(",")) {
                        scale = getPositiveInt();
                        original += ", " + scale;
                    } else {
                        scale = 0;
                    }
                }
                original += ")";
                read(")");
            }
        } else if(readIf("(")) {
            // support for MySQL: INT(11), MEDIUMINT(8) and so on. Just ignore the precision.
            getPositiveInt();
            read(")");
        }
        if(readIf("FOR")) {
            read("BIT");
            read("DATA");
            if(dataType.type == Value.STRING) {
                dataType = DataType.getTypeByName("BINARY");
            }
        }
        int type = dataType.type;
        Column column = new Column(columnName, type, precision, scale);
        if(templateColumn != null) {
            column.setNullable(templateColumn.getNullable());
            column.setDefaultExpression(session, templateColumn.getDefaultExpression());
            int selectivity = templateColumn.getSelectivity();
            if(selectivity != Constants.SELECTIVITY_DEFAULT) {
                column.setSelectivity(selectivity);
            }
            Expression checkConstraint = templateColumn.getCheckConstraint(session, columnName);
            if(checkConstraint != null) {
                column.addCheckConstraint(session, checkConstraint);
            }
        }
        column.setOriginalSQL(original);
        return column;
    }

    private Prepared parseCreate() throws SQLException {
        boolean force = readIf("FORCE");
        if(readIf("LOCAL")) {
            read("TEMPORARY");
            read("TABLE");
            return parseCreateTable(true, false, false);
        } else if(readIf("GLOBAL")) {
            read("TEMPORARY");
            read("TABLE");
            return parseCreateTable(true, true, false);
        } else if(readIf("TEMP") || readIf("TEMPORARY")) {
            read("TABLE");
            return parseCreateTable(true, true, false);
        } else if (readIf("MEMORY")) {
            read("TABLE");
            return parseCreateTable(false, false, false);
        } else if(readIf("LINKED")) {
            return parseCreateLinkedTable();
        } else if (readIf("CACHED")) {
            read("TABLE");
            return parseCreateTable(false, false, true);
        } else if (readIf("TABLE")) {
            int defaultMode;
            Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_TABLE_TYPE));
            defaultMode = setting == null ? Constants.DEFAULT_TABLE_TYPE : setting.getIntValue();
            return parseCreateTable(false, false, defaultMode==Table.TYPE_CACHED);
        } else if(readIf("VIEW")) {
            return parseCreateView(force);
        } else if (readIf("ALIAS")) {
             return parseCreateFunctionAlias();
        } else if (readIf("SEQUENCE")) {
            return  parseCreateSequence();
        } else if (readIf("USER")) {
            return parseCreateUser();
        } else if (readIf("TRIGGER")) {
            return parseCreateTrigger();
        } else if (readIf("ROLE")) {
            return parseCreateRole();
        } else if(readIf("SCHEMA")) {
            return parseCreateSchema();
        } else if(readIf("CONSTANT")) {
            return parseCreateConstant();
        } else if(readIf("DOMAIN")) {
            return parseCreateUserDataType();
        } else if(readIf("TYPE")) {
            return parseCreateUserDataType();
        } else if(readIf("DATATYPE")) {
            return parseCreateUserDataType();
        } else {
            boolean hash = false, primaryKey = false, unique = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if(readIf("PRIMARY")) {
                read("KEY");
                if(readIf("HASH")) {
                    hash = true;
                }
                primaryKey = true;
            } else {
                if (readIf("UNIQUE")) {
                    unique = true;
                    if(readIf("HASH")) {
                        hash = true;
                    }
                } if(readIf("INDEX")) {
                    if(!isToken("ON")) {
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
            command.setColumnNames(parseColumnList(true));
            return command;
        }
    }

    private boolean addRoleOrRight(GrantRevoke command) throws SQLException {
        if(readIf("SELECT")) {
            command.addRight(Right.SELECT);
            return false;
        } else if(readIf("DELETE")) {
            command.addRight(Right.DELETE);
            return false;
        } else if(readIf("INSERT")) {
            command.addRight(Right.INSERT);
            return false;
        } else if(readIf("UPDATE")) {
            command.addRight(Right.UPDATE);
            return false;
        } else if(readIf("ALL")) {
            command.addRight(Right.ALL);
            return false;
        } else if(readIf("CONNECT")) {
            // ignore this right
            return false;
        } else if(readIf("RESOURCE")) {
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
        while(readIf(",")) {
            boolean next = addRoleOrRight(command);
            if(next != isRoleBased) {
                throw Message.getSQLException(Message.ROLES_AND_RIGHT_CANNOT_BE_MIXED);
            }
        }
        if(!isRoleBased) {
            if(readIf("ON")) {
                do {
                    String tableName = readIdentifierWithSchema();
                    Table table = getSchema().getTableOrView(session, tableName);
                    command.addTable(table);
                } while(readIf(","));
            }
        }
        if(operationType == GrantRevoke.GRANT) {
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
        if(readIf("AUTHORIZATION")) {
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
        if(readIf("START")) {
            read("WITH");
            long start = readLong();
            command.setStartWith(start);
        }
        if(readIf("INCREMENT")) {
            read("BY");
            long increment = readLong();
            command.setIncrement(increment);
        }
        if(readIf("BELONGS_TO_TABLE")) {
            command.setBelongsToTable(true);
        }
        return command;
    }

    private boolean readIfNoExists() throws SQLException {
        if(readIf("IF")) {
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
        read("VALUE");
        Expression expr = readExpression();
        CreateConstant command = new CreateConstant(session, schema);
        command.setConstantName(constantName);
        command.setExpression(expr);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateUserDataType parseCreateUserDataType() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        CreateUserDataType command = new CreateUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        read("AS");
        Column col = parseColumnForTable("VALUE");
        if(readIf("CHECK")) {
            Expression expr = readExpression();
            col.addCheckConstraint(session, expr);
        }
        col.rename(null);
        command.setColumn(col);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateTrigger parseCreateTrigger() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String triggerName = readIdentifierWithSchema(null);
        Schema schema = getSchema();
        boolean isBefore;
        if(readIf("BEFORE")) {
            isBefore = true;
        } else {
            read("AFTER");
            isBefore = false;
        }
        int typeMask = 0;
        do {
            if(readIf("INSERT")) {
                typeMask |= TriggerObject.INSERT;
            } else if(readIf("UPDATE")) {
                typeMask |= TriggerObject.UPDATE;
            } else if(readIf("DELETE")) {
                typeMask |= TriggerObject.DELETE;
            } else {
                throw getSyntaxError();
            }
        } while(readIf(","));
        read("ON");
        String tableName = readIdentifierWithSchema();
        checkSchema(schema);
        CreateTrigger command = new CreateTrigger(session, getSchema());
        command.setTriggerName(triggerName);
        command.setIfNotExists(ifNotExists);
        command.setBefore(isBefore);
        command.setTypeMask(typeMask);
        command.setTableName(tableName);
        if(readIf("FOR")) {
            read("EACH");
            read("ROW");
            command.setRowBased(true);
        } else {
            command.setRowBased(false);
        }
        if(readIf("QUEUE")) {
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
        if(readIf("PASSWORD")) {
            command.setPassword(readString());
        } else if(readIf("SALT")) {
            command.setSalt(readString());
            read("HASH");
            command.setHash(readString());
        } else if(readIf("IDENTIFIED")) {
            read("BY");
            // uppercase if not quoted
            command.setPassword(readColumnIdentifier());
        } else {
            throw getSyntaxError();
        }
        if(readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private CreateFunctionAlias parseCreateFunctionAlias() throws SQLException {
        boolean ifNotExists = readIfNoExists();
        CreateFunctionAlias command = new CreateFunctionAlias(session);
        command.setAliasName(readUniqueIdentifier());
        command.setIfNotExists(ifNotExists);
        read("FOR");
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private CreateView parseCreateView(boolean force) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String viewName = readIdentifierWithSchema();
        CreateView command = new CreateView(session, getSchema());
        command.setForce(force);
        command.setViewName(viewName);
        command.setIfNotExists(ifNotExists);
        String select = StringCache.getNew(sqlCommand.substring(parseIndex));
        command.setComment(readCommentIf());
        if(readIf("(")) {
            String[] cols = parseColumnList(false);
            command.setColumnNames(cols);
        }
        read("AS");
        try {
            Query query = parseSelect();
            query.prepare();
            command.setSelect(query);
        } catch(SQLException e) {
            if(force) {
                command.setSelectSQL(select);
            } else {
                throw e;
            }
        }
        return command;
    }

    private TransactionCommand parseCheckpoint() throws SQLException {
        TransactionCommand command;
        if(readIf("SYNC")) {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT_SYNC);
        } else {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT);
        }
        return command;
    }

    private Prepared parseAlter() throws SQLException {
        if(readIf("TABLE")) {
            return parseAlterTable();
        } else if(readIf("USER")) {
            return parseAlterUser();
        } else if(readIf("INDEX")) {
            return parseAlterIndex();
        } else if(readIf("SEQUENCE")) {
            return parseAlterSequence();
        } else if(readIf("VIEW")) {
            return parseAlterView();
        }
        throw getSyntaxError();
    }

    private void checkSchema(Schema old) throws SQLException {
        if(old != null && getSchema() != old) {
            throw Message.getSQLException(Message.SCHEMA_NAME_MUST_MATCH);
        }
    }

    private AlterIndexRename parseAlterIndex() throws SQLException {
        String indexName = readIdentifierWithSchema();
        Schema old = getSchema();
        AlterIndexRename command = new AlterIndexRename(session, getSchema());
        command.setOldIndex(getSchema().getIndex(indexName));
        read("RENAME");
        read("TO");
        String newName = readIdentifierWithSchema(old.getSQL());
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private AlterView parseAlterView() throws SQLException {
        AlterView command = new AlterView(session);
        String viewName = readIdentifierWithSchema();
        Table tableView = getSchema().findTableOrView(session, viewName);
        if(!(tableView instanceof TableView)) {
            throw Message.getSQLException(Message.VIEW_NOT_FOUND_1, viewName);
        }
        TableView view = (TableView)tableView;
        command.setView(view);
        read("RECOMPILE");
        return command;
    }

    private AlterSequence parseAlterSequence() throws SQLException {
        AlterSequence command = new AlterSequence(session);
        String sequenceName = readIdentifierWithSchema();
        command.setSequence(getSchema().getSequence(sequenceName));
        if(readIf("RESTART")) {
            read("WITH");
            long start = readLong();
            command.setStartWith(start);
        }
        if(readIf("INCREMENT")) {
            read("BY");
            long increment = readLong();
            command.setIncrement(increment);
        }
        return command;
    }

    private AlterUser parseAlterUser() throws SQLException {
        String userName = readUniqueIdentifier();
        if(readIf("SET")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            User user = database.getUser(userName);
            command.setUser(user);
            if(readIf("PASSWORD")) {
                command.setPassword(readString());
            } else if(readIf("SALT")) {
                command.setSalt(readString());
                read("HASH");
                command.setHash(readString());
            } else {
                throw getSyntaxError();
            }
            return command;
        } else if(readIf("RENAME")) {
            read("TO");
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.RENAME);
            command.setUser(database.getUser(userName));
            String newName = readUniqueIdentifier();
            command.setNewName(newName);
            return command;
        } else if(readIf("ADMIN")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.ADMIN);
            User user = database.getUser(userName);
            command.setUser(user);
            if(readIf("TRUE"))  {
                command.setAdmin(true);
            } else if(readIf("FALSE")) {
                command.setAdmin(false);
            } else {
                throw getSyntaxError();
            }
            return command;
        }
        throw getSyntaxError();
    }

    private Prepared parseSet() throws SQLException {
        if(readIf("AUTOCOMMIT")) {
            boolean value = readBooleanSetting();
            int setting = value ? TransactionCommand.AUTOCOMMIT_TRUE : TransactionCommand.AUTOCOMMIT_FALSE;
            return new TransactionCommand(session, setting);
        } else if(readIf("IGNORECASE")) {
            boolean value = readBooleanSetting();
            Set command = new Set(session, SetTypes.IGNORECASE);
            command.setInt(value ? 1 : 0);
            return command;
        } else if(readIf("PASSWORD")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setPassword(readString());
            return command;
        } else if(readIf("SALT")) {
            AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setSalt(readString());
            read("HASH");
            command.setHash(readString());
            return command;
        } else if(readIf("MODE")) {
            Set command = new Set(session, SetTypes.MODE);
            command.setString(readAliasIdentifier());
            return command;
        } else if(readIf("COMPRESS_LOB")) {
            Set command = new Set(session, SetTypes.COMPRESS_LOB);
            if(currentTokenType == VALUE) {
                command.setString(readString());
            } else {
                command.setString(readUniqueIdentifier());
            }
            return command;
        } else if(readIf("DATABASE")) {
            read("COLLATION");
            return parseSetCollation();
        } else if(readIf("COLLATION")) {
            return parseSetCollation();
        } else if(readIf("CLUSTER")) {
            Set command = new Set(session, SetTypes.CLUSTER);
            command.setString(readString());
            return command;
        } else if(readIf("DATABASE_EVENT_LISTENER")) {
            Set command = new Set(session, SetTypes.DATABASE_EVENT_LISTENER);
            command.setString(readString());
            return command;
        } else if(readIf("ALLOW_LITERALS")) {
            Set command = new Set(session, SetTypes.ALLOW_LITERALS);
            if(readIf("NONE")) {
                command.setInt(Constants.ALLOW_LITERALS_NONE);
            } else if(readIf("ALL")) {
                command.setInt(Constants.ALLOW_LITERALS_ALL);
            } else if(readIf("NUMBERS")){
                command.setInt(Constants.ALLOW_LITERALS_NUMBERS);
            } else {
                command.setInt(getPositiveInt());
            }
            return command;
        } else if(readIf("DEFAULT_TABLE_TYPE")) {
            Set command = new Set(session, SetTypes.DEFAULT_TABLE_TYPE);
            if(readIf("MEMORY")) {
                command.setInt(Table.TYPE_MEMORY);
            } else if(readIf("CACHED")) {
                command.setInt(Table.TYPE_CACHED);
            } else {
                command.setInt(getPositiveInt());
            }
            return command;
        } else if(readIf("CREATE")) {
            // Derby compatibility (CREATE=TRUE in the database URL)
            read();
            return new NoOperation(session);
        } else if(readIf("HSQLDB.DEFAULT_TABLE_TYPE")) {
            read();
            return new NoOperation(session);
        } else if(readIf("CACHE_TYPE")) {
            read();
            return new NoOperation(session);
        } else if(readIf("FILE_LOCK")) {
            read();
            return new NoOperation(session);
        } else if(readIf("STORAGE")) {
            read();
            return new NoOperation(session);
        } else if(readIf("DB_CLOSE_ON_EXIT")) {
            read();
            return new NoOperation(session);
        } else if(readIf("RECOVER")) {
            read();
            return new NoOperation(session);            
        } else if(readIf("SCHEMA")) {
            Set command = new Set(session, SetTypes.SCHEMA);
            command.setString(readAliasIdentifier());
            return command;
        } else {
            if(isToken("LOGSIZE")) {
                // HSQLDB compatibility
                currentToken = SetTypes.getTypeName(SetTypes.MAX_LOG_SIZE);
            }
            int type = SetTypes.getType(currentToken);
            if(type >= 0) {
                read();
                Set command = new Set(session, type);
                command.setExpression(readExpression());
                return command;
            } else {
                throw getSyntaxError();
            }
        }
    }

    private Set parseSetCollation() throws SQLException {
        Set command = new Set(session, SetTypes.COLLATION);
        String name = readAliasIdentifier();
        command.setString(name);
        if(name.equals(CompareMode.OFF)) {
            return command;
        }
        Collator coll = CompareMode.getCollator(name);
        if(coll == null) {
            throw getSyntaxError();
        }
        if(readIf("STRENGTH")) {
            if(readIf("PRIMARY")) {
                command.setInt(Collator.PRIMARY);
            } else if(readIf("SECONDARY")) {
                command.setInt(Collator.SECONDARY);
            } else if(readIf("TERTIARY")) {
                command.setInt(Collator.TERTIARY);
            } else if(readIf("IDENTICAL")) {
                command.setInt(Collator.IDENTICAL);
            }
        } else {
            command.setInt(coll.getStrength());
        }
        return command;
    }

    private RunScript parseRunScript() throws SQLException {
        RunScript command = new RunScript(session);
        read("FROM");
        command.setFileName(readString());
        if(readIf("COMPRESSION")) {
            command.setCompressionAlgorithm(readUniqueIdentifier());
        }
        if(readIf("CIPHER")) {
            command.setCipher(readUniqueIdentifier());
            if(readIf("PASSWORD")) {
                command.setPassword(readString().toCharArray());
            }
        }
        if(readIf("CHARSET")) {
            command.setCharset(readString());
        }
        return command;
    }

    private Script parseScript() throws SQLException {
        Script command = new Script(session);
        boolean data = true, passwords = true, settings = true, dropTables = false;
        if(readIf("NODATA")) {
            data = false;
        }
        if(readIf("NOPASSWORDS")) {
            passwords = false;
        }
        if(readIf("NOSETTINGS")) {
            settings = false;
        }
        if(readIf("DROP")) {
            dropTables = true;
        }
        if(readIf("BLOCKSIZE")) {
            long blockSize = readLong();
            command.setLobBlockSize(blockSize);
        }
        command.setData(data);
        command.setPasswords(passwords);
        command.setSettings(settings);
        command.setDrop(dropTables);
        if(readIf("TO")) {
            command.setFileName(readString());
            if(readIf("COMPRESSION")) {
                command.setCompressionAlgorithm(readUniqueIdentifier());
            }
            if(readIf("CIPHER")) {
                command.setCipher(readUniqueIdentifier());
                if(readIf("PASSWORD")) {
                    command.setPassword(readString().toCharArray());
                }
            }
        }
        return command;
    }

    private Prepared parseAlterTable() throws SQLException {
        String tableName = readIdentifierWithSchema();
        Schema tableSchema = getSchema();
        Table table = getSchema().getTableOrView(session, tableName);
        if(readIf("ADD")) {
            Prepared command = parseAlterTableAddConstraintIf(getSchema(), tableName);
            if(command != null) {
                return command;
            }
            return parseAlterTableAddColumn(table);
        } else if(readIf("SET")) {
            read("REFERENTIAL_INTEGRITY");
            int type;
            if(readIf("TRUE")) {
                type = AlterTableAddConstraint.REFERENTIAL_INTEGRITY_TRUE;
            } else {
                read("FALSE");
                type = AlterTableAddConstraint.REFERENTIAL_INTEGRITY_FALSE;
            }
            AlterTableAddConstraint command = new AlterTableAddConstraint(session, getSchema());
            command.setTableName(tableName);
            command.setType(type);
            return command;
        } else if(readIf("RENAME")) {
            read("TO");
            String newName = readIdentifierWithSchema(tableSchema.getSQL());
            checkSchema(tableSchema);
            AlterTableRename command = new AlterTableRename(session, getSchema());
            command.setOldTable(table);
            command.setNewTableName(newName);
            return command;
        } else if(readIf("DROP")) {
            if(readIf("CONSTRAINT")) {
                String constraintName = readIdentifierWithSchema(tableSchema.getSQL());
                checkSchema(tableSchema);
                AlterTableDropConstraint command = new AlterTableDropConstraint(session, getSchema());
                command.setConstraintName(constraintName);
                return command;
            } else if(readIf("PRIMARY")) {
                read("KEY");
                Index idx = table.getPrimaryKey();
                DropIndex command = new DropIndex(session, tableSchema);
                command.setIndexName(idx.getName());
                return command;
            } else {
                readIf("COLUMN");
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
                command.setType(AlterTableAlterColumn.DROP);
                String columnName = readColumnIdentifier();
                command.setTable(table);
                command.setOldColumn(table.getColumn(columnName));
                return command;
            }
        } else if(readIf("ALTER")) {
            readIf("COLUMN");
            String columnName = readColumnIdentifier();
            Column column = table.getColumn(columnName);
            if(readIf("RENAME")) {
                read("TO");
                AlterTableRenameColumn command = new AlterTableRenameColumn(session);
                command.setTable(table);
                command.setColumn(column);
                String newName = readColumnIdentifier();
                command.setNewColumnName(newName);
                return command;
            } else if(readIf("SET")) {
                if(readIf("DATA")) {
                    // Derby compatibility
                    read("TYPE");
                    Column newColumn = parseColumnForTable(columnName);
                    AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
                    command.setTable(table);
                    command.setType(AlterTableAlterColumn.CHANGE_TYPE);
                    command.setOldColumn(column);
                    command.setNewColumn(newColumn);
                    return command;
                }
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
                command.setTable(table);
                command.setOldColumn(column);
                if(readIf("NULL")) {
                    command.setType(AlterTableAlterColumn.NULL);
                    return command;
                } else if(readIf("NOT")) {
                    read("NULL");
                    command.setType(AlterTableAlterColumn.NOT_NULL);
                    return command;
                } else if(readIf("DEFAULT")) {
                    Expression defaultExpression = readExpression();
                    command.setType(AlterTableAlterColumn.DEFAULT);
                    command.setDefaultExpression(defaultExpression);
                    return command;
                }
            } else if(readIf("RESTART")) {
                readIf("WITH");
                long start = readLong();
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
                command.setTable(table);
                command.setType(AlterTableAlterColumn.RESTART);
                command.setOldColumn(column);
                command.setStartWith(start);
                return command;
            } else if(readIf("SELECTIVITY")) {
                int sel = getPositiveInt();
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
                command.setTable(table);
                command.setType(AlterTableAlterColumn.SELECTIVITY);
                command.setOldColumn(column);
                command.setStartWith(sel);
                return command;
            } else {
                Column newColumn = parseColumnForTable(columnName);
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, tableSchema);
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
        if(readIf("BEFORE")) {
            command.setAddBefore(readColumnIdentifier());
        }
        return command;
    }

    private int parseAction() throws SQLException {
        if(readIf("CASCADE")) {
            return ConstraintReferential.CASCADE;
        } else if(readIf("RESTRICT")) {
            return ConstraintReferential.RESTRICT;
        } else if(readIf("NO")) {
            read("ACTION");
            return ConstraintReferential.RESTRICT;
        } else {
            read("SET");
            if(readIf("NULL")) {
                return ConstraintReferential.SET_NULL;
            } else {
                read("DEFAULT");
                return ConstraintReferential.SET_DEFAULT;
            }
        }
    }

    private Prepared parseAlterTableAddConstraintIf(Schema schema, String tableName) throws SQLException {
        String name = null, comment = null;
        if(readIf("CONSTRAINT")) {
            name = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            comment = readCommentIf();
        }
        if(readIf("PRIMARY")) {
            read("KEY");
            CreateIndex command = new CreateIndex(session, schema);
            command.setComment(comment);
            command.setTableName(tableName);
            command.setPrimaryKey(true);
            if(readIf("HASH")) {
                command.setHash(true);
            }
            read("(");
            command.setColumnNames(parseColumnList(true));
            return command;
        } else if(Mode.getCurrentMode().indexDefinitionInCreateTable && (readIf("INDEX") || readIf("KEY"))) {
            // MySQL
            CreateIndex command = new CreateIndex(session, schema);
            command.setComment(comment);
            command.setTableName(tableName);
            if(!readIf("(")) {
                command.setIndexName(readUniqueIdentifier());
                read("(");
            }
            command.setColumnNames(parseColumnList(true));
            return command;
        }
        AlterTableAddConstraint command;
        if(readIf("CHECK")) {
            command = new AlterTableAddConstraint(session, schema);
            command.setType(AlterTableAddConstraint.CHECK);
            command.setCheckExpression(readExpression());
        } else if(readIf("UNIQUE")) {
            readIf("INDEX");
            command = new AlterTableAddConstraint(session, schema);
            command.setType(AlterTableAddConstraint.UNIQUE);
            if(!readIf("(")) {
                name = readUniqueIdentifier();
                read("(");
            }
            command.setColumnNames(parseColumnList(true));
            if(readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(indexName));
            }
        } else if(readIf("FOREIGN")) {
            command = new AlterTableAddConstraint(session, schema);
            command.setType(AlterTableAddConstraint.REFERENTIAL);
            read("KEY");
            read("(");
            String[] cols = parseColumnList(true);
            command.setColumnNames(cols);
            if(readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(schema.findIndex(indexName));
            }
            read("REFERENCES");
            if(readIf("(")) {
                command.setRefTableName(schema, tableName);
                cols = parseColumnList(false);
                command.setRefColumnNames(cols);
            } else {
                String refTableName = readIdentifierWithSchema(schema.getName());
                command.setRefTableName(getSchema(), refTableName);
                if(readIf("(")) {
                    cols = parseColumnList(false);
                    command.setRefColumnNames(cols);
                }
            }
            if(readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setRefIndex(getSchema().findIndex(indexName));
            }
            while(readIf("ON")) {
                if(readIf("DELETE")) {
                    command.setDeleteAction(parseAction());
                } else {
                    read("UPDATE");
                    command.setUpdateAction(parseAction());
                }
            }
            if(readIf("NOT")) {
                read("DEFERRABLE");
            } else {
                readIf("DEFERRABLE");
            }
        } else {
            if(name != null) {
                throw getSyntaxError();
            }
            return null;
        }
        command.setTableName(tableName);
        command.setConstraintName(name);
        command.setComment(comment);
        return command;
    }

    private CreateLinkedTable parseCreateLinkedTable() throws SQLException {
        read("TABLE");
        boolean ifNotExists = readIfNoExists();
        String tableName = readIdentifierWithSchema();
        CreateLinkedTable command = new CreateLinkedTable(session, getSchema());
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
        command.setOriginalTable(readString());
        read(")");
        return command;
    }

    private CreateTable parseCreateTable(boolean temp, boolean globalTemp, boolean persistent) throws SQLException {
        boolean ifNotExists = readIfNoExists();
        String tableName = readIdentifierWithSchema();
        if(temp && globalTemp && "SESSION".equals(schemaName)) {
            // support weird syntax: declare global temporary table session.xy (...) not logged
            schemaName = session.getCurrentSchemaName();
            globalTemp = false;
        }
        Schema schema = getSchema();
        CreateTable command = new CreateTable(session, schema);
        command.setPersistent(persistent);
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        if(readIf("AS")) {
            Query query = parseQueryWithParams();
            command.setQuery(query);
        } else {
            read("(");
            if(!readIf(")")) {
                do {
                    Prepared c = parseAlterTableAddConstraintIf(schema, tableName);
                    if(c != null) {
                        command.addConstraintCommand(c);
                    } else {
                        String columnName = readColumnIdentifier();
                        Column column = parseColumnForTable(columnName);
                        if(column.getAutoIncrement()) {
                            command.setPrimaryKeyColumnNames(new String[]{column.getName()});
                        }
                        command.addColumn(column);
                        String constraintName = null;
                        if(readIf("CONSTRAINT")) {
                            constraintName = readColumnIdentifier();
                        }
                        if (readIf("PRIMARY")) {
                            read("KEY");
                            if(readIf("HASH")) {
                                command.setHashPrimaryKey(true);
                            }
                            command.setPrimaryKeyColumnNames(new String[]{column.getName()});
                        } else if(readIf("UNIQUE")) {
                            AlterTableAddConstraint unique = new AlterTableAddConstraint(session, schema);
                            unique.setConstraintName(constraintName);
                            unique.setType(AlterTableAddConstraint.UNIQUE);
                            unique.setColumnNames(new String[]{columnName});
                            unique.setTableName(tableName);
                            command.addConstraintCommand(unique);
                        } else if(readIf("CHECK")) {
                            Expression expr = readExpression();
                            column.addCheckConstraint(session, expr);
                        } else if(readIf("REFERENCES")) {
                            AlterTableAddConstraint ref = new AlterTableAddConstraint(session, schema);
                            ref.setConstraintName(constraintName);
                            ref.setType(AlterTableAddConstraint.REFERENTIAL);
                            ref.setColumnNames(new String[]{columnName});
                            ref.setTableName(tableName);
                            String refTableName = readIdentifierWithSchema(schema.getName());
                            ref.setRefTableName(getSchema(), refTableName);
                            if(readIf("(")) {
                                String[] cols = parseColumnList(false);
                                ref.setRefColumnNames(cols);
                            }
                            command.addConstraintCommand(ref);
                        }
                    }
                } while(readIf(","));
                read(")");
            }
        }
        if(temp) {
            if(readIf("ON")) {
                read("COMMIT");
                if(readIf("DROP")) {
                    command.setOnCommitDrop();
                } else if(readIf("DELETE")) {
                    read("ROWS");
                    command.setOnCommitTruncate();
                }
            } else if(readIf("NOT")) {
                read("LOGGED");
            }
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

    public static String quoteIdentifier(String s) {
        if(s == null || s.length()==0) {
            return "\"\"";
        }
        char c = s.charAt(0);
        // lowercase a-z is quoted as well
        if((!Character.isLetter(c) && c != '_') || Character.isLowerCase(c)) {
            return StringUtils.quoteIdentifier(s);
        }
        for(int i=0; i<s.length(); i++) {
            c = s.charAt(i);
            if((!Character.isLetterOrDigit(c) && c != '_') || Character.isLowerCase(c)) {
                return StringUtils.quoteIdentifier(s);
            }
        }
        if(Parser.isKeyword(s)) {
            return StringUtils.quoteIdentifier(s);
        }
        return s;
    }

    public void setRightsChecked(boolean rightsChecked) {
        this.rightsChecked = rightsChecked;
    }

    public Expression parseExpression(String sql) throws SQLException {
        initialize(sql);
        read();
        return readExpression();
    }

}
