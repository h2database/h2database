/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 * Support for the operator "&&" as an alias for SPATIAL_INTERSECTS
 */
package org.h2.command;

import static org.h2.util.ParserUtil.ALL;
import static org.h2.util.ParserUtil.AND;
import static org.h2.util.ParserUtil.ANY;
import static org.h2.util.ParserUtil.ARRAY;
import static org.h2.util.ParserUtil.AS;
import static org.h2.util.ParserUtil.ASYMMETRIC;
import static org.h2.util.ParserUtil.AUTHORIZATION;
import static org.h2.util.ParserUtil.BETWEEN;
import static org.h2.util.ParserUtil.CASE;
import static org.h2.util.ParserUtil.CAST;
import static org.h2.util.ParserUtil.CHECK;
import static org.h2.util.ParserUtil.CONSTRAINT;
import static org.h2.util.ParserUtil.CROSS;
import static org.h2.util.ParserUtil.CURRENT_CATALOG;
import static org.h2.util.ParserUtil.CURRENT_DATE;
import static org.h2.util.ParserUtil.CURRENT_PATH;
import static org.h2.util.ParserUtil.CURRENT_ROLE;
import static org.h2.util.ParserUtil.CURRENT_SCHEMA;
import static org.h2.util.ParserUtil.CURRENT_TIME;
import static org.h2.util.ParserUtil.CURRENT_TIMESTAMP;
import static org.h2.util.ParserUtil.CURRENT_USER;
import static org.h2.util.ParserUtil.DAY;
import static org.h2.util.ParserUtil.DEFAULT;
import static org.h2.util.ParserUtil.DISTINCT;
import static org.h2.util.ParserUtil.ELSE;
import static org.h2.util.ParserUtil.END;
import static org.h2.util.ParserUtil.EXCEPT;
import static org.h2.util.ParserUtil.EXISTS;
import static org.h2.util.ParserUtil.FALSE;
import static org.h2.util.ParserUtil.FETCH;
import static org.h2.util.ParserUtil.FIRST_KEYWORD;
import static org.h2.util.ParserUtil.FOR;
import static org.h2.util.ParserUtil.FOREIGN;
import static org.h2.util.ParserUtil.FROM;
import static org.h2.util.ParserUtil.FULL;
import static org.h2.util.ParserUtil.GROUP;
import static org.h2.util.ParserUtil.HAVING;
import static org.h2.util.ParserUtil.HOUR;
import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.IF;
import static org.h2.util.ParserUtil.IN;
import static org.h2.util.ParserUtil.INNER;
import static org.h2.util.ParserUtil.INTERSECT;
import static org.h2.util.ParserUtil.INTERSECTS;
import static org.h2.util.ParserUtil.INTERVAL;
import static org.h2.util.ParserUtil.IS;
import static org.h2.util.ParserUtil.JOIN;
import static org.h2.util.ParserUtil.KEY;
import static org.h2.util.ParserUtil.LAST_KEYWORD;
import static org.h2.util.ParserUtil.LEFT;
import static org.h2.util.ParserUtil.LIKE;
import static org.h2.util.ParserUtil.LIMIT;
import static org.h2.util.ParserUtil.LOCALTIME;
import static org.h2.util.ParserUtil.LOCALTIMESTAMP;
import static org.h2.util.ParserUtil.MINUS;
import static org.h2.util.ParserUtil.MINUTE;
import static org.h2.util.ParserUtil.MONTH;
import static org.h2.util.ParserUtil.NATURAL;
import static org.h2.util.ParserUtil.NOT;
import static org.h2.util.ParserUtil.NULL;
import static org.h2.util.ParserUtil.OFFSET;
import static org.h2.util.ParserUtil.ON;
import static org.h2.util.ParserUtil.OR;
import static org.h2.util.ParserUtil.ORDER;
import static org.h2.util.ParserUtil.PRIMARY;
import static org.h2.util.ParserUtil.QUALIFY;
import static org.h2.util.ParserUtil.RIGHT;
import static org.h2.util.ParserUtil.ROW;
import static org.h2.util.ParserUtil.ROWNUM;
import static org.h2.util.ParserUtil.SECOND;
import static org.h2.util.ParserUtil.SELECT;
import static org.h2.util.ParserUtil.SESSION_USER;
import static org.h2.util.ParserUtil.SET;
import static org.h2.util.ParserUtil.SOME;
import static org.h2.util.ParserUtil.SYMMETRIC;
import static org.h2.util.ParserUtil.SYSTEM_USER;
import static org.h2.util.ParserUtil.TABLE;
import static org.h2.util.ParserUtil.TO;
import static org.h2.util.ParserUtil.TRUE;
import static org.h2.util.ParserUtil.UNION;
import static org.h2.util.ParserUtil.UNIQUE;
import static org.h2.util.ParserUtil.UNKNOWN;
import static org.h2.util.ParserUtil.USER;
import static org.h2.util.ParserUtil.USING;
import static org.h2.util.ParserUtil.VALUE;
import static org.h2.util.ParserUtil.VALUES;
import static org.h2.util.ParserUtil.WHEN;
import static org.h2.util.ParserUtil.WHERE;
import static org.h2.util.ParserUtil.WINDOW;
import static org.h2.util.ParserUtil.WITH;
import static org.h2.util.ParserUtil.YEAR;
import static org.h2.util.ParserUtil._ROWID_;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.api.Trigger;
import org.h2.command.ddl.AlterDomainExpressions;
import org.h2.command.ddl.AlterDomainAddConstraint;
import org.h2.command.ddl.AlterDomainDropConstraint;
import org.h2.command.ddl.AlterDomainRename;
import org.h2.command.ddl.AlterDomainRenameConstraint;
import org.h2.command.ddl.AlterIndexRename;
import org.h2.command.ddl.AlterSchemaRename;
import org.h2.command.ddl.AlterSequence;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.AlterTableDropConstraint;
import org.h2.command.ddl.AlterTableRename;
import org.h2.command.ddl.AlterTableRenameColumn;
import org.h2.command.ddl.AlterTableRenameConstraint;
import org.h2.command.ddl.AlterUser;
import org.h2.command.ddl.AlterView;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CommandWithColumns;
import org.h2.command.ddl.CreateAggregate;
import org.h2.command.ddl.CreateConstant;
import org.h2.command.ddl.CreateDomain;
import org.h2.command.ddl.CreateFunctionAlias;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateLinkedTable;
import org.h2.command.ddl.CreateRole;
import org.h2.command.ddl.CreateSchema;
import org.h2.command.ddl.CreateSequence;
import org.h2.command.ddl.CreateSynonym;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.CreateTrigger;
import org.h2.command.ddl.CreateUser;
import org.h2.command.ddl.CreateView;
import org.h2.command.ddl.DeallocateProcedure;
import org.h2.command.ddl.DefineCommand;
import org.h2.command.ddl.DropAggregate;
import org.h2.command.ddl.DropConstant;
import org.h2.command.ddl.DropDatabase;
import org.h2.command.ddl.DropDomain;
import org.h2.command.ddl.DropFunctionAlias;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropRole;
import org.h2.command.ddl.DropSchema;
import org.h2.command.ddl.DropSequence;
import org.h2.command.ddl.DropSynonym;
import org.h2.command.ddl.DropTable;
import org.h2.command.ddl.DropTrigger;
import org.h2.command.ddl.DropUser;
import org.h2.command.ddl.DropView;
import org.h2.command.ddl.GrantRevoke;
import org.h2.command.ddl.PrepareProcedure;
import org.h2.command.ddl.SequenceOptions;
import org.h2.command.ddl.SetComment;
import org.h2.command.ddl.TruncateTable;
import org.h2.command.dml.AlterTableSet;
import org.h2.command.dml.BackupCommand;
import org.h2.command.dml.Call;
import org.h2.command.dml.CommandWithValues;
import org.h2.command.dml.DataChangeStatement;
import org.h2.command.dml.Delete;
import org.h2.command.dml.ExecuteImmediate;
import org.h2.command.dml.ExecuteProcedure;
import org.h2.command.dml.Explain;
import org.h2.command.dml.Help;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.MergeUsing;
import org.h2.command.dml.NoOperation;
import org.h2.command.dml.RunScriptCommand;
import org.h2.command.dml.ScriptCommand;
import org.h2.command.dml.Set;
import org.h2.command.dml.SetClauseList;
import org.h2.command.dml.SetSessionCharacteristics;
import org.h2.command.dml.SetTypes;
import org.h2.command.dml.TransactionCommand;
import org.h2.command.dml.Update;
import org.h2.command.query.Query;
import org.h2.command.query.QueryOrderBy;
import org.h2.command.query.Select;
import org.h2.command.query.SelectUnion;
import org.h2.command.query.TableValueConstructor;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.DbSettings;
import org.h2.engine.IsolationLevel;
import org.h2.engine.Mode;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.Procedure;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.expression.Alias;
import org.h2.expression.ArrayConstructorByQuery;
import org.h2.expression.ArrayElementReference;
import org.h2.expression.BinaryOperation;
import org.h2.expression.BinaryOperation.OpType;
import org.h2.expression.ConcatenationOperation;
import org.h2.expression.DomainValueExpression;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.ExpressionWithVariableParameters;
import org.h2.expression.FieldReference;
import org.h2.expression.Format;
import org.h2.expression.Format.FormatEnum;
import org.h2.expression.Parameter;
import org.h2.expression.Rownum;
import org.h2.expression.SearchedCase;
import org.h2.expression.SequenceValue;
import org.h2.expression.SimpleCase;
import org.h2.expression.Subquery;
import org.h2.expression.TimeZoneOperation;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.UnaryOperation;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.expression.Wildcard;
import org.h2.expression.aggregate.AbstractAggregate;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.JavaAggregate;
import org.h2.expression.aggregate.ListaggArguments;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.Window;
import org.h2.expression.analysis.WindowFrame;
import org.h2.expression.analysis.WindowFrameBound;
import org.h2.expression.analysis.WindowFrameBoundType;
import org.h2.expression.analysis.WindowFrameExclusion;
import org.h2.expression.analysis.WindowFrameUnits;
import org.h2.expression.analysis.WindowFunction;
import org.h2.expression.analysis.WindowFunctionType;
import org.h2.expression.condition.BetweenPredicate;
import org.h2.expression.condition.BooleanTest;
import org.h2.expression.condition.CompareLike;
import org.h2.expression.condition.CompareLike.LikeType;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.ConditionIn;
import org.h2.expression.condition.ConditionInParameter;
import org.h2.expression.condition.ConditionInQuery;
import org.h2.expression.condition.ConditionLocalAndGlobal;
import org.h2.expression.condition.ConditionNot;
import org.h2.expression.condition.ExistsPredicate;
import org.h2.expression.condition.IsJsonPredicate;
import org.h2.expression.condition.NullPredicate;
import org.h2.expression.condition.TypePredicate;
import org.h2.expression.condition.UniquePredicate;
import org.h2.expression.function.ArrayFunction;
import org.h2.expression.function.BitFunction;
import org.h2.expression.function.BuiltinFunctions;
import org.h2.expression.function.CSVWriteFunction;
import org.h2.expression.function.CardinalityExpression;
import org.h2.expression.function.CastSpecification;
import org.h2.expression.function.CoalesceFunction;
import org.h2.expression.function.CompatibilitySequenceValueFunction;
import org.h2.expression.function.CompressFunction;
import org.h2.expression.function.ConcatFunction;
import org.h2.expression.function.CryptFunction;
import org.h2.expression.function.CurrentDateTimeValueFunction;
import org.h2.expression.function.CurrentGeneralValueSpecification;
import org.h2.expression.function.DBObjectFunction;
import org.h2.expression.function.DataTypeSQLFunction;
import org.h2.expression.function.DateTimeFormatFunction;
import org.h2.expression.function.DateTimeFunction;
import org.h2.expression.function.DayMonthNameFunction;
import org.h2.expression.function.FileFunction;
import org.h2.expression.function.HashFunction;
import org.h2.expression.function.JavaFunction;
import org.h2.expression.function.JsonConstructorFunction;
import org.h2.expression.function.LengthFunction;
import org.h2.expression.function.MathFunction;
import org.h2.expression.function.MathFunction1;
import org.h2.expression.function.MathFunction2;
import org.h2.expression.function.NullIfFunction;
import org.h2.expression.function.RandFunction;
import org.h2.expression.function.RegexpFunction;
import org.h2.expression.function.SessionControlFunction;
import org.h2.expression.function.SetFunction;
import org.h2.expression.function.SignalFunction;
import org.h2.expression.function.SoundexFunction;
import org.h2.expression.function.StringFunction;
import org.h2.expression.function.StringFunction1;
import org.h2.expression.function.StringFunction2;
import org.h2.expression.function.SubstringFunction;
import org.h2.expression.function.SysInfoFunction;
import org.h2.expression.function.TableInfoFunction;
import org.h2.expression.function.ToCharFunction;
import org.h2.expression.function.TrimFunction;
import org.h2.expression.function.TruncateValueFunction;
import org.h2.expression.function.XMLFunction;
import org.h2.expression.function.table.ArrayTableFunction;
import org.h2.expression.function.table.CSVReadFunction;
import org.h2.expression.function.table.JavaTableFunction;
import org.h2.expression.function.table.LinkSchemaFunction;
import org.h2.expression.function.table.TableFunction;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mode.FunctionsPostgreSQL;
import org.h2.mode.ModeFunction;
import org.h2.mode.OnDuplicateKeyValues;
import org.h2.mode.Regclass;
import org.h2.result.SortOrder;
import org.h2.schema.Domain;
import org.h2.schema.FunctionAlias;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.schema.UserAggregate;
import org.h2.schema.UserDefinedFunction;
import org.h2.table.Column;
import org.h2.table.DataChangeDeltaTable;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.DualTable;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.IndexHints;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.util.HasSQL;
import org.h2.util.IntervalUtils;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.json.JSONItemType;
import org.h2.util.json.JsonConstructorUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.ExtTypeInfoGeometry;
import org.h2.value.ExtTypeInfoNumeric;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInteger;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueRow;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * The parser is used to convert a SQL statement string to an command object.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class Parser {

    private static final String WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS =
            "WITH statement supports only SELECT, TABLE, VALUES, " +
            "CREATE TABLE, INSERT, UPDATE, MERGE or DELETE statements";

    // used during the tokenizer phase
    private static final int CHAR_END = 1, CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5,
            CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DOT = 8,
            CHAR_DOLLAR_QUOTED_STRING = 9;

    // these are token types, see also types in ParserUtil

    /**
     * Token with parameter.
     */
    private static final int PARAMETER = LAST_KEYWORD + 1;

    /**
     * End of input.
     */
    private static final int END_OF_INPUT = PARAMETER + 1;

    /**
     * Token with literal.
     */
    private static final int LITERAL = END_OF_INPUT + 1;

    /**
     * The token "=".
     */
    private static final int EQUAL = LITERAL + 1;

    /**
     * The token ">=".
     */
    private static final int BIGGER_EQUAL = EQUAL + 1;

    /**
     * The token ">".
     */
    private static final int BIGGER = BIGGER_EQUAL + 1;

    /**
     * The token "<".
     */
    private static final int SMALLER = BIGGER + 1;

    /**
     * The token "<=".
     */
    private static final int SMALLER_EQUAL = SMALLER + 1;

    /**
     * The token "<>" or "!=".
     */
    private static final int NOT_EQUAL = SMALLER_EQUAL + 1;

    /**
     * The token "@".
     */
    private static final int AT = NOT_EQUAL + 1;

    /**
     * The token "-".
     */
    private static final int MINUS_SIGN = AT + 1;

    /**
     * The token "+".
     */
    private static final int PLUS_SIGN = MINUS_SIGN + 1;

    /**
     * The token "||".
     */
    private static final int CONCATENATION = PLUS_SIGN + 1;

    /**
     * The token "(".
     */
    private static final int OPEN_PAREN = CONCATENATION + 1;

    /**
     * The token ")".
     */
    private static final int CLOSE_PAREN = OPEN_PAREN + 1;

    /**
     * The token &amp;.
     */
    private static final int AMPERSAND = CLOSE_PAREN + 1;

    /**
     * The token "&amp;&amp;".
     */
    private static final int SPATIAL_INTERSECTS = AMPERSAND + 1;

    /**
     * The token "*".
     */
    private static final int ASTERISK = SPATIAL_INTERSECTS + 1;

    /**
     * The token ",".
     */
    private static final int COMMA = ASTERISK + 1;

    /**
     * The token ".".
     */
    private static final int DOT = COMMA + 1;

    /**
     * The token "{".
     */
    private static final int OPEN_BRACE = DOT + 1;

    /**
     * The token "}".
     */
    private static final int CLOSE_BRACE = OPEN_BRACE + 1;

    /**
     * The token "/".
     */
    private static final int SLASH = CLOSE_BRACE + 1;

    /**
     * The token "%".
     */
    private static final int PERCENT = SLASH + 1;

    /**
     * The token ";".
     */
    private static final int SEMICOLON = PERCENT + 1;

    /**
     * The token ":".
     */
    private static final int COLON = SEMICOLON + 1;

    /**
     * The token "[".
     */
    private static final int OPEN_BRACKET = COLON + 1;

    /**
     * The token "]".
     */
    private static final int CLOSE_BRACKET = OPEN_BRACKET + 1;

    /**
     * The token "~".
     */
    private static final int TILDE = CLOSE_BRACKET + 1;

    /**
     * The token "::".
     */
    private static final int COLON_COLON = TILDE + 1;

    /**
     * The token ":=".
     */
    private static final int COLON_EQ = COLON_COLON + 1;

    /**
     * The token "!~".
     */
    private static final int NOT_TILDE = COLON_EQ + 1;

    private static final String[] TOKENS = {
            // Unused
            null,
            // KEYWORD
            null,
            // IDENTIFIER
            null,
            // ALL
            "ALL",
            // AND
            "AND",
            // ANY
            "ANY",
            // ARRAY
            "ARRAY",
            // AS
            "AS",
            // ASYMMETRIC
            "ASYMMETRIC",
            // AUTHORIZATION
            "AUTHORIZATION",
            // BETWEEN
            "BETWEEN",
            // CASE
            "CASE",
            // CAST
            "CAST",
            // CHECK
            "CHECK",
            // CONSTRAINT
            "CONSTRAINT",
            // CROSS
            "CROSS",
            // CURRENT_CATALOG
            "CURRENT_CATALOG",
            // CURRENT_DATE
            "CURRENT_DATE",
            // CURRENT_PATH
            "CURRENT_PATH",
            // CURRENT_ROLE
            "CURRENT_ROLE",
            // CURRENT_SCHEMA
            "CURRENT_SCHEMA",
            // CURRENT_TIME
            "CURRENT_TIME",
            // CURRENT_TIMESTAMP
            "CURRENT_TIMESTAMP",
            // CURRENT_USER
            "CURRENT_USER",
            // DAY
            "DAY",
            // DEFAULT
            "DEFAULT",
            // DISTINCT
            "DISTINCT",
            // ELSE
            "ELSE",
            // END
            "END",
            // EXCEPT
            "EXCEPT",
            // EXISTS
            "EXISTS",
            // FALSE
            "FALSE",
            // FETCH
            "FETCH",
            // FOR
            "FOR",
            // FOREIGN
            "FOREIGN",
            // FROM
            "FROM",
            // FULL
            "FULL",
            // GROUP
            "GROUP",
            // HAVING
            "HAVING",
            // HOUR
            "HOUR",
            // IF
            "IF",
            // IN
            "IN",
            // INNER
            "INNER",
            // INTERSECT
            "INTERSECT",
            // INTERSECTS
            "INTERSECTS",
            // INTERVAL
            "INTERVAL",
            // IS
            "IS",
            // JOIN
            "JOIN",
            // KEY
            "KEY",
            // LEFT
            "LEFT",
            // LIKE
            "LIKE",
            // LIMIT
            "LIMIT",
            // LOCALTIME
            "LOCALTIME",
            // LOCALTIMESTAMP
            "LOCALTIMESTAMP",
            // MINUS
            "MINUS",
            // MINUTE
            "MINUTE",
            // MONTH
            "MONTH",
            // NATURAL
            "NATURAL",
            // NOT
            "NOT",
            // NULL
            "NULL",
            // OFFSET
            "OFFSET",
            // ON
            "ON",
            // OR
            "OR",
            // ORDER
            "ORDER",
            // PRIMARY
            "PRIMARY",
            // QUALIFY
            "QUALIFY",
            // RIGHT
            "RIGHT",
            // ROW
            "ROW",
            // ROWNUM
            "ROWNUM",
            // SECOND
            "SECOND",
            // SELECT
            "SELECT",
            // SESSION_USER
            "SESSION_USER",
            // SET
            "SET",
            // SOME
            "SOME",
            // SYMMETRIC
            "SYMMETRIC",
            // SYSTEM_USER
            "SYSTEM_USER",
            // TABLE
            "TABLE",
            // TO
            "TO",
            // TRUE
            "TRUE",
            // UESCAPE
            "UESCAPE",
            // UNION
            "UNION",
            // UNIQUE
            "UNIQUE",
            // UNKNOWN
            "UNKNOWN",
            // USER
            "USER",
            // USING
            "USING",
            // VALUE
            "VALUE",
            // VALUES
            "VALUES",
            // WHEN
            "WHEN",
            // WHERE
            "WHERE",
            // WINDOW
            "WINDOW",
            // WITH
            "WITH",
            // YEAR
            "YEAR",
            // _ROWID_
            "_ROWID_",
            // PARAMETER
            "?",
            // END
            null,
            // VALUE
            null,
            // EQUAL
            "=",
            // BIGGER_EQUAL
            ">=",
            // BIGGER
            ">",
            // SMALLER
            "<",
            // SMALLER_EQUAL
            "<=",
            // NOT_EQUAL
            "<>",
            // AT
            "@",
            // MINUS_SIGN
            "-",
            // PLUS_SIGN
            "+",
            // STRING_CONCAT
            "||",
            // OPEN_PAREN
            "(",
            // CLOSE_PAREN
            ")",
            // SPATIAL_INTERSECTS
            "&&",
            // ASTERISK
            "*",
            // COMMA
            ",",
            // DOT
            ".",
            // OPEN_BRACE
            "{",
            // CLOSE_BRACE
            "}",
            // SLASH
            "/",
            // PERCENT
            "%",
            // SEMICOLON
            ";",
            // COLON
            ":",
            // OPEN_BRACKET
            "[",
            // CLOSE_BRACKET
            "]",
            // TILDE
            "~",
            // COLON_COLON
            "::",
            // COLON_EQ
            ":=",
            // NOT_TILDE
            "!~",
            // End
    };

    private static final Comparator<TableFilter> TABLE_FILTER_COMPARATOR = (o1, o2) -> {
        if (o1 == o2)
            return 0;
        assert o1.getOrderInFrom() != o2.getOrderInFrom();
        return o1.getOrderInFrom() > o2.getOrderInFrom() ? 1 : -1;
    };

    private final Database database;
    private final SessionLocal session;

    /**
     * @see org.h2.engine.DbSettings#databaseToLower
     */
    private final boolean identifiersToLower;
    /**
     * @see org.h2.engine.DbSettings#databaseToUpper
     */
    private final boolean identifiersToUpper;

    /**
     * @see org.h2.engine.SessionLocal#isVariableBinary()
     */
    private final boolean variableBinary;

    private final BitSet nonKeywords;

    /** indicates character-type for each char in sqlCommand */
    private int[] characterTypes;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private Value currentValue;
    private String originalSQL;
    /** copy of originalSQL, with comments blanked out */
    private String sqlCommand;
    /** cached array if chars from sqlCommand */
    private char[] sqlCommandChars;
    /** index into sqlCommand of previous token */
    private int lastParseIndex;
    /** index into sqlCommand of current token */
    private int parseIndex;
    private CreateView createView;
    private Prepared currentPrepared;
    private Select currentSelect;
    private List<TableView> cteCleanups;
    private ArrayList<Parameter> parameters;
    private ArrayList<Parameter> indexedParameterList;
    private ArrayList<Parameter> suppliedParameters;
    private ArrayList<Parameter> suppliedParameterList;
    private String schemaName;
    private ArrayList<String> expectedList;
    private boolean rightsChecked;
    private boolean recompileAlways;
    private boolean literalsChecked;
    private int orderInFrom;
    private boolean parseDomainConstraint;

    /**
     * Parses the specified collection of non-keywords.
     *
     * @param nonKeywords array of non-keywords in upper case
     * @return bit set of non-keywords, or {@code null}
     */
    public static BitSet parseNonKeywords(String[] nonKeywords) {
        if (nonKeywords.length == 0) {
            return null;
        }
        BitSet set = new BitSet();
        for (String nonKeyword : nonKeywords) {
            int index = Arrays.binarySearch(TOKENS, FIRST_KEYWORD, LAST_KEYWORD + 1, nonKeyword);
            if (index >= 0) {
                set.set(index);
            }
        }
        return set.isEmpty() ? null : set;
    }

    /**
     * Formats a comma-separated list of keywords.
     *
     * @param nonKeywords bit set of non-keywords, or {@code null}
     * @return comma-separated list of non-keywords
     */
    public static String formatNonKeywords(BitSet nonKeywords) {
        if (nonKeywords == null || nonKeywords.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = -1; (i = nonKeywords.nextSetBit(i + 1)) >= 0;) {
            if (i >= FIRST_KEYWORD && i <= LAST_KEYWORD) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append(TOKENS[i]);
            }
        }
        return builder.toString();
    }

    /**
     * Creates a new instance of parser.
     *
     * @param session the session
     */
    public Parser(SessionLocal session) {
        this.database = session.getDatabase();
        DbSettings settings = database.getSettings();
        this.identifiersToLower = settings.databaseToLower;
        this.identifiersToUpper = settings.databaseToUpper;
        this.variableBinary = session.isVariableBinary();
        this.nonKeywords = session.getNonKeywords();
        this.session = session;
    }

    /**
     * Creates a new instance of parser for special use cases.
     */
    public Parser() {
        database = null;
        identifiersToLower = false;
        identifiersToUpper = false;
        variableBinary = false;
        nonKeywords = null;
        session = null;
    }

    /**
     * Parse the statement and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    public Prepared prepare(String sql) {
        Prepared p = parse(sql);
        p.prepare();
        if (currentTokenType != END_OF_INPUT) {
            throw getSyntaxError();
        }
        return p;
    }

    /**
     * Parse a statement or a list of statements, and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the command object
     */
    public Command prepareCommand(String sql) {
        try {
            Prepared p = parse(sql);
            if (currentTokenType != SEMICOLON && currentTokenType != END_OF_INPUT) {
                addExpected(SEMICOLON);
                throw getSyntaxError();
            }
            try {
                p.prepare();
            } catch (Throwable t) {
                CommandContainer.clearCTE(session, p);
                throw t;
            }
            if (parseIndex < sql.length()) {
                sql = sql.substring(0, parseIndex);
            }
            CommandContainer c = new CommandContainer(session, sql, p);
            if (currentTokenType == SEMICOLON) {
                String remaining = originalSQL.substring(parseIndex);
                if (!StringUtils.isWhitespaceOrEmpty(remaining)) {
                    return prepareCommandList(c, p, sql, remaining);
                }
            }
            return c;
        } catch (DbException e) {
            throw e.addSQL(originalSQL);
        }
    }

    private CommandList prepareCommandList(CommandContainer command, Prepared p, String sql, String remaining) {
        try {
            ArrayList<Prepared> list = Utils.newSmallArrayList();
            do {
                if (p instanceof DefineCommand) {
                    // Next commands may depend on results of this command.
                    return new CommandList(session, sql, command, list, parameters, remaining);
                }
                suppliedParameters = parameters;
                suppliedParameterList = indexedParameterList;
                try {
                    p = parse(remaining);
                } catch (DbException ex) {
                    // This command may depend on results of previous commands.
                    if (ex.getErrorCode() == ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS) {
                        throw ex;
                    }
                    return new CommandList(session, sql, command, list, parameters, remaining);
                }
                list.add(p);
                if (currentTokenType == END_OF_INPUT) {
                    break;
                }
                if (currentTokenType != SEMICOLON) {
                    addExpected(SEMICOLON);
                    throw getSyntaxError();
                }
            } while (!StringUtils.isWhitespaceOrEmpty(remaining = originalSQL.substring(parseIndex)));
            return new CommandList(session, sql, command, list, parameters, null);
        } catch (Throwable t) {
            command.clearCTE();
            throw t;
        }
    }

    /**
     * Parse the statement, but don't prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    Prepared parse(String sql) {
        Prepared p;
        try {
            // first, try the fast variant
            p = parse(sql, false);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.SYNTAX_ERROR_1) {
                // now, get the detailed exception
                p = parse(sql, true);
            } else {
                throw e.addSQL(sql);
            }
        }
        p.setPrepareAlways(recompileAlways);
        p.setParameterList(parameters);
        return p;
    }

    private Prepared parse(String sql, boolean withExpectedList) {
        initialize(sql);
        if (withExpectedList) {
            expectedList = new ArrayList<>();
        } else {
            expectedList = null;
        }
        parameters = suppliedParameters != null ? suppliedParameters : Utils.newSmallArrayList();
        indexedParameterList = suppliedParameterList;
        currentSelect = null;
        currentPrepared = null;
        createView = null;
        cteCleanups = null;
        recompileAlways = false;
        read();
        Prepared p;
        try {
            p = parsePrepared();
            p.setCteCleanups(cteCleanups);
        } catch (Throwable t) {
            if (cteCleanups != null) {
                CommandContainer.clearCTE(session, cteCleanups);
            }
            throw t;
        }
        return p;
    }

    private Prepared parsePrepared() {
        int start = lastParseIndex;
        Prepared c = null;
        switch (currentTokenType) {
        case END_OF_INPUT:
        case SEMICOLON:
            c = new NoOperation(session);
            setSQL(c, start);
            return c;
        case PARAMETER:
            // read the ? as a parameter
            readTerm();
            // this is an 'out' parameter - set a dummy value
            parameters.get(0).setValue(ValueNull.INSTANCE);
            read(EQUAL);
            read("CALL");
            c = parseCall();
            break;
        case OPEN_PAREN:
        case SELECT:
        case TABLE:
        case VALUES:
            c = parseQuery();
            break;
        case WITH:
            read();
            c = parseWithStatementOrQuery(start);
            break;
        case SET:
            read();
            c = parseSet();
            break;
        case IDENTIFIER:
            if (currentTokenQuoted) {
                break;
            }
            /*
             * Convert a-z to A-Z. This method is safe, because only A-Z
             * characters are considered below.
             *
             * Unquoted identifier is never empty.
             */
            switch (currentToken.charAt(0) & 0xffdf) {
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
                    c = parseCall();
                } else if (readIf("CHECKPOINT")) {
                    c = parseCheckpoint();
                } else if (readIf("COMMENT")) {
                    c = parseComment();
                }
                break;
            case 'D':
                if (readIf("DELETE")) {
                    c = parseDelete(start);
                } else if (readIf("DROP")) {
                    c = parseDrop();
                } else if (readIf("DECLARE")) {
                    // support for DECLARE GLOBAL TEMPORARY TABLE...
                    c = parseCreate();
                } else if (database.getMode().getEnum() != ModeEnum.MSSQLServer && readIf("DEALLOCATE")) {
                    /*
                     * PostgreSQL-style DEALLOCATE is disabled in MSSQLServer
                     * mode because PostgreSQL-style EXECUTE is redefined in
                     * this mode.
                     */
                    c = parseDeallocate();
                }
                break;
            case 'E':
                if (readIf("EXPLAIN")) {
                    c = parseExplain();
                } else if (database.getMode().getEnum() != ModeEnum.MSSQLServer) {
                    if (readIf("EXECUTE")) {
                        c = parseExecutePostgre();
                    }
                } else {
                    if (readIf("EXEC") || readIf("EXECUTE")) {
                        c = parseExecuteSQLServer();
                    }
                }
                break;
            case 'G':
                if (readIf("GRANT")) {
                    c = parseGrantRevoke(CommandInterface.GRANT);
                }
                break;
            case 'H':
                if (readIf("HELP")) {
                    c = parseHelp();
                }
                break;
            case 'I':
                if (readIf("INSERT")) {
                    c = parseInsert(start);
                }
                break;
            case 'M':
                if (readIf("MERGE")) {
                    c = parseMerge(start);
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
                    c = parseGrantRevoke(CommandInterface.REVOKE);
                } else if (readIf("RUNSCRIPT")) {
                    c = parseRunScript();
                } else if (readIf("RELEASE")) {
                    c = parseReleaseSavepoint();
                } else if (database.getMode().replaceInto && readIf("REPLACE")) {
                    c = parseReplace(start);
                }
                break;
            case 'S':
                if (readIf("SAVEPOINT")) {
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
                    c = parseUpdate(start);
                } else if (readIf("USE")) {
                    c = parseUse();
                }
                break;
            }
        }
        if (c == null) {
            throw getSyntaxError();
        }
        if (indexedParameterList != null) {
            for (int i = 0, size = indexedParameterList.size();
                    i < size; i++) {
                if (indexedParameterList.get(i) == null) {
                    indexedParameterList.set(i, new Parameter(i));
                }
            }
            parameters = indexedParameterList;
        }
        boolean withParamValues = readIf(OPEN_BRACE);
        if (withParamValues) {
            do {
                int index = (int) readLong() - 1;
                if (index < 0 || index >= parameters.size()) {
                    throw getSyntaxError();
                }
                Parameter p = parameters.get(index);
                if (p == null) {
                    throw getSyntaxError();
                }
                read(COLON);
                Expression expr = readExpression();
                expr = expr.optimize(session);
                p.setValue(expr.getValue(session));
            } while (readIf(COMMA));
            read(CLOSE_BRACE);
            for (Parameter p : parameters) {
                p.checkSet();
            }
            parameters.clear();
        }
        if (withParamValues || c.getSQL() == null) {
            setSQL(c, start);
        }
        return c;
    }

    private DbException getSyntaxError() {
        if (expectedList == null || expectedList.isEmpty()) {
            return DbException.getSyntaxError(sqlCommand, parseIndex);
        }
        return DbException.getSyntaxError(sqlCommand, parseIndex,
                StringUtils.join(new StringBuilder(), expectedList, ", ").toString());
    }

    private Prepared parseBackup() {
        BackupCommand command = new BackupCommand(session);
        read(TO);
        command.setFileName(readExpression());
        return command;
    }

    private Prepared parseAnalyze() {
        Analyze command = new Analyze(session);
        if (readIf(TABLE)) {
            Table table = readTableOrView();
            command.setTable(table);
        }
        if (readIf("SAMPLE_SIZE")) {
            command.setTop(readNonNegativeInt());
        }
        return command;
    }

    private TransactionCommand parseBegin() {
        TransactionCommand command;
        if (!readIf("WORK")) {
            readIf("TRANSACTION");
        }
        command = new TransactionCommand(session, CommandInterface.BEGIN);
        return command;
    }

    private TransactionCommand parseCommit() {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, CommandInterface.COMMIT_TRANSACTION);
            command.setTransactionName(readIdentifier());
            return command;
        }
        command = new TransactionCommand(session, CommandInterface.COMMIT);
        readIf("WORK");
        return command;
    }

    private TransactionCommand parseShutdown() {
        int type = CommandInterface.SHUTDOWN;
        if (readIf("IMMEDIATELY")) {
            type = CommandInterface.SHUTDOWN_IMMEDIATELY;
        } else if (readIf("COMPACT")) {
            type = CommandInterface.SHUTDOWN_COMPACT;
        } else if (readIf("DEFRAG")) {
            type = CommandInterface.SHUTDOWN_DEFRAG;
        } else {
            readIf("SCRIPT");
        }
        return new TransactionCommand(session, type);
    }

    private TransactionCommand parseRollback() {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, CommandInterface.ROLLBACK_TRANSACTION);
            command.setTransactionName(readIdentifier());
            return command;
        }
        readIf("WORK");
        if (readIf(TO)) {
            read("SAVEPOINT");
            command = new TransactionCommand(session, CommandInterface.ROLLBACK_TO_SAVEPOINT);
            command.setSavepointName(readIdentifier());
        } else {
            command = new TransactionCommand(session, CommandInterface.ROLLBACK);
        }
        return command;
    }

    private Prepared parsePrepare() {
        if (readIf("COMMIT")) {
            TransactionCommand command = new TransactionCommand(session, CommandInterface.PREPARE_COMMIT);
            command.setTransactionName(readIdentifier());
            return command;
        }
        return parsePrepareProcedure();
    }

    private Prepared parsePrepareProcedure() {
        if (database.getMode().getEnum() == ModeEnum.MSSQLServer) {
            throw getSyntaxError();
            /*
             * PostgreSQL-style PREPARE is disabled in MSSQLServer mode
             * because PostgreSQL-style EXECUTE is redefined in this
             * mode.
             */
        }
        String procedureName = readIdentifier();
        if (readIf(OPEN_PAREN)) {
            ArrayList<Column> list = Utils.newSmallArrayList();
            for (int i = 0;; i++) {
                Column column = parseColumnForTable("C" + i, true);
                list.add(column);
                if (!readIfMore()) {
                    break;
                }
            }
        }
        read(AS);
        Prepared prep = parsePrepared();
        PrepareProcedure command = new PrepareProcedure(session);
        command.setProcedureName(procedureName);
        command.setPrepared(prep);
        return command;
    }

    private TransactionCommand parseSavepoint() {
        TransactionCommand command = new TransactionCommand(session, CommandInterface.SAVEPOINT);
        command.setSavepointName(readIdentifier());
        return command;
    }

    private Prepared parseReleaseSavepoint() {
        Prepared command = new NoOperation(session);
        readIf("SAVEPOINT");
        readIdentifier();
        return command;
    }

    private Schema findSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = database.findSchema(schemaName);
        if (schema == null) {
            if (equalsToken("SESSION", schemaName)) {
                // for local temporary tables
                schema = database.getSchema(session.getCurrentSchemaName());
            }
        }
        return schema;
    }

    private Schema getSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    private Schema getSchema() {
        return getSchema(schemaName);
    }
    /*
     * Gets the current schema for scenarios that need a guaranteed, non-null schema object.
     *
     * This routine is solely here
     * because of the function readIdentifierWithSchema(String defaultSchemaName) - which
     * is often called with a null parameter (defaultSchemaName) - then 6 lines into the function
     * that routine nullifies the state field schemaName - which I believe is a bug.
     *
     * There are about 7 places where "readIdentifierWithSchema(null)" is called in this file.
     *
     * In other words when is it legal to not have an active schema defined by schemaName ?
     * I don't think it's ever a valid case. I don't understand when that would be allowed.
     * I spent a long time trying to figure this out.
     * As another proof of this point, the command "SET SCHEMA=NULL" is not a valid command.
     *
     * I did try to fix this in readIdentifierWithSchema(String defaultSchemaName)
     * - but every fix I tried cascaded so many unit test errors - so
     * I gave up. I think this needs a bigger effort to fix his, as part of bigger, dedicated story.
     *
     */
    private Schema getSchemaWithDefault() {
        if (schemaName == null) {
            schemaName = session.getCurrentSchemaName();
        }
        return getSchema(schemaName);
    }

    private Column readTableColumn(TableFilter filter) {
        String columnName = readIdentifier();
        if (readIf(DOT)) {
            columnName = readTableColumn(filter, columnName);
        }
        return filter.getTable().getColumn(columnName);
    }

    private String readTableColumn(TableFilter filter, String tableAlias) {
        String columnName = readIdentifier();
        if (readIf(DOT)) {
            String schema = tableAlias;
            tableAlias = columnName;
            columnName = readIdentifier();
            if (readIf(DOT)) {
                checkDatabaseName(schema);
                schema = tableAlias;
                tableAlias = columnName;
                columnName = readIdentifier();
            }
            if (!equalsToken(schema, filter.getTable().getSchema().getName())) {
                throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schema);
            }
        }
        if (!equalsToken(tableAlias, filter.getTableAlias())) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
        }
        return columnName;
    }

    private Update parseUpdate(int start) {
        Update command = new Update(session);
        currentPrepared = command;
        Expression fetch = null;
        if (database.getMode().topInDML && readIf("TOP")) {
            read(OPEN_PAREN);
            fetch = readTerm().optimize(session);
            read(CLOSE_PAREN);
        }
        TableFilter filter = readSimpleTableFilter();
        command.setTableFilter(filter);
        command.setSetClauseList(readUpdateSetClause(filter));
        if (database.getMode().allowUsingFromClauseInUpdateStatement && readIf(FROM)) {
            TableFilter fromTable = readTableFilter();
            command.setFromTableFilter(fromTable);
        }
        if (readIf(WHERE)) {
            command.setCondition(readExpression());
        }
        if (fetch == null) {
            // for MySQL compatibility
            // (this syntax is supported, but ignored)
            readIfOrderBy();
            fetch = readFetchOrLimit();
        }
        command.setFetch(fetch);
        setSQL(command, start);
        return command;
    }

    private SetClauseList readUpdateSetClause(TableFilter filter) {
        read(SET);
        SetClauseList list = new SetClauseList(filter.getTable());
        do {
            if (readIf(OPEN_PAREN)) {
                ArrayList<Column> columns = Utils.newSmallArrayList();
                do {
                    columns.add(readTableColumn(filter));
                } while (readIfMore());
                read(EQUAL);
                list.addMultiple(columns, readExpression());
            } else {
                Column column = readTableColumn(filter);
                read(EQUAL);
                list.addSingle(column, readExpressionOrDefault());
            }
        } while (readIf(COMMA));
        return list;
    }

    private TableFilter readSimpleTableFilter() {
        return new TableFilter(session, readTableOrView(), readFromAlias(null), rightsChecked, currentSelect, 0, null);
    }

    private Delete parseDelete(int start) {
        Delete command = new Delete(session);
        Expression fetch = null;
        if (database.getMode().topInDML && readIf("TOP")) {
            fetch = readTerm().optimize(session);
        }
        currentPrepared = command;
        if (!readIf(FROM) && database.getMode().getEnum() == ModeEnum.MySQL) {
            readIdentifierWithSchema();
            read(FROM);
        }
        command.setTableFilter(readSimpleTableFilter());
        if (readIf(WHERE)) {
            command.setCondition(readExpression());
        }
        if (fetch == null) {
            fetch = readFetchOrLimit();
        }
        command.setFetch(fetch);
        setSQL(command, start);
        return command;
    }

    private Expression readFetchOrLimit() {
        Expression fetch = null;
        if (readIf(FETCH)) {
            if (!readIf("FIRST")) {
                read("NEXT");
            }
            if (readIf(ROW) || readIf("ROWS")) {
                fetch = ValueExpression.get(ValueInteger.get(1));
            } else {
                fetch = readExpression().optimize(session);
                if (!readIf(ROW)) {
                    read("ROWS");
                }
            }
            read("ONLY");
        } else if (database.getMode().limit && readIf(LIMIT)) {
            fetch = readTerm().optimize(session);
        }
        return fetch;
    }

    private IndexColumn[] parseIndexColumnList() {
        ArrayList<IndexColumn> columns = Utils.newSmallArrayList();
        do {
            columns.add(new IndexColumn(readIdentifier(), parseSortType()));
        } while (readIfMore());
        return columns.toArray(new IndexColumn[0]);
    }

    private int parseSortType() {
        int sortType = !readIf("ASC") && readIf("DESC") ? SortOrder.DESCENDING : SortOrder.ASCENDING;
        if (readIf("NULLS")) {
            if (readIf("FIRST")) {
                sortType |= SortOrder.NULLS_FIRST;
            } else {
                read("LAST");
                sortType |= SortOrder.NULLS_LAST;
            }
        }
        return sortType;
    }

    private String[] parseColumnList() {
        ArrayList<String> columns = Utils.newSmallArrayList();
        do {
            columns.add(readIdentifier());
        } while (readIfMore());
        return columns.toArray(new String[0]);
    }

    private Column[] parseColumnList(Table table) {
        ArrayList<Column> columns = Utils.newSmallArrayList();
        HashSet<Column> set = new HashSet<>();
        if (!readIf(CLOSE_PAREN)) {
            do {
                Column column = parseColumn(table);
                if (!set.add(column)) {
                    throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getTraceSQL());
                }
                columns.add(column);
            } while (readIfMore());
        }
        return columns.toArray(new Column[0]);
    }

    private Column parseColumn(Table table) {
        if (currentTokenType == _ROWID_) {
            read();
            return table.getRowIdColumn();
        }
        return table.getColumn(readIdentifier());
    }

    /**
     * Read comma or closing brace.
     *
     * @return {@code true} if comma is read, {@code false} if brace is read
     */
    private boolean readIfMore() {
        if (readIf(COMMA)) {
            return true;
        }
        read(CLOSE_PAREN);
        return false;
    }

    private Prepared parseHelp() {
        HashSet<String> conditions = new HashSet<>();
        while (currentTokenType != END_OF_INPUT) {
            conditions.add(StringUtils.toUpperEnglish(currentToken));
            read();
        }
        return new Help(session, conditions.toArray(new String[0]));
    }

    private Prepared parseShow() {
        ArrayList<Value> paramValues = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder("SELECT ");
        if (readIf("CLIENT_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UNICODE' CLIENT_ENCODING");
        } else if (readIf("DEFAULT_TRANSACTION_ISOLATION")) {
            // for PostgreSQL compatibility
            buff.append("'read committed' DEFAULT_TRANSACTION_ISOLATION");
        } else if (readIf("TRANSACTION")) {
            // for PostgreSQL compatibility
            read("ISOLATION");
            read("LEVEL");
            buff.append("LOWER(ISOLATION_LEVEL) TRANSACTION_ISOLATION FROM INFORMATION_SCHEMA.SESSIONS "
                    + "WHERE SESSION_ID = SESSION_ID()");
        } else if (readIf("DATESTYLE")) {
            // for PostgreSQL compatibility
            buff.append("'ISO' DATESTYLE");
        } else if (readIf("SEARCH_PATH")) {
            // for PostgreSQL compatibility
            String[] searchPath = session.getSchemaSearchPath();
            StringBuilder searchPathBuff = new StringBuilder();
            if (searchPath != null) {
                for (int i = 0; i < searchPath.length; i ++) {
                    if (i > 0) {
                        searchPathBuff.append(", ");
                    }
                    ParserUtil.quoteIdentifier(searchPathBuff, searchPath[i], HasSQL.QUOTE_ONLY_WHEN_REQUIRED);
                }
            }
            StringUtils.quoteStringSQL(buff, searchPathBuff.toString());
            buff.append(" SEARCH_PATH");
        } else if (readIf("SERVER_VERSION")) {
            // for PostgreSQL compatibility
            buff.append("'" + Constants.PG_VERSION + "' SERVER_VERSION");
        } else if (readIf("SERVER_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UTF8' SERVER_ENCODING");
        } else if (readIf("SSL")) {
            // for PostgreSQL compatibility
            buff.append("'off' SSL");
        } else if (readIf("TABLES")) {
            // for MySQL compatibility
            String schema = database.getMainSchema().getName();
            if (readIf(FROM)) {
                schema = readIdentifier();
            }
            buff.append("TABLE_NAME, TABLE_SCHEMA FROM "
                    + "INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME");
            paramValues.add(ValueVarchar.get(schema));
        } else if (readIf("COLUMNS")) {
            // for MySQL compatibility
            read(FROM);
            String tableName = readIdentifierWithSchema();
            String schemaName = getSchema().getName();
            paramValues.add(ValueVarchar.get(tableName));
            if (readIf(FROM)) {
                schemaName = readIdentifier();
            }
            buff.append("C.COLUMN_NAME FIELD, ");
            boolean oldInformationSchema = session.isOldInformationSchema();
            buff.append(oldInformationSchema
                    ? "C.COLUMN_TYPE"
                    : "DATA_TYPE_SQL(?2, ?1, 'TABLE', C.DTD_IDENTIFIER)");
            buff.append(" TYPE, "
                    + "C.IS_NULLABLE \"NULL\", "
                    + "CASE (SELECT MAX(I.INDEX_TYPE_NAME) FROM "
                    + "INFORMATION_SCHEMA.INDEXES I ");
            if (!oldInformationSchema) {
                buff.append("JOIN INFORMATION_SCHEMA.INDEX_COLUMNS IC ");
            }
            buff.append("WHERE I.TABLE_SCHEMA=C.TABLE_SCHEMA "
                    + "AND I.TABLE_NAME=C.TABLE_NAME ");
            if (oldInformationSchema) {
                buff.append("AND I.COLUMN_NAME=C.COLUMN_NAME");
            } else {
                buff.append("AND IC.TABLE_SCHEMA=C.TABLE_SCHEMA "
                        + "AND IC.TABLE_NAME=C.TABLE_NAME "
                        + "AND IC.INDEX_SCHEMA=I.INDEX_SCHEMA "
                        + "AND IC.INDEX_NAME=I.INDEX_NAME "
                        + "AND IC.COLUMN_NAME=C.COLUMN_NAME");
            }
            buff.append(')'
                    + "WHEN 'PRIMARY KEY' THEN 'PRI' "
                    + "WHEN 'UNIQUE INDEX' THEN 'UNI' ELSE '' END `KEY`, "
                    + "COALESCE(COLUMN_DEFAULT, 'NULL') `DEFAULT` "
                    + "FROM INFORMATION_SCHEMA.COLUMNS C "
                    + "WHERE C.TABLE_NAME=?1 AND C.TABLE_SCHEMA=?2 "
                    + "ORDER BY C.ORDINAL_POSITION");
            paramValues.add(ValueVarchar.get(schemaName));
        } else if (readIf("DATABASES") || readIf("SCHEMAS")) {
            // for MySQL compatibility
            buff.append("SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        } else if (database.getMode().getEnum() == ModeEnum.PostgreSQL && readIf("ALL")) {
            // for PostgreSQL compatibility
            buff.append("NAME, SETTING FROM PG_CATALOG.PG_SETTINGS");
        }
        boolean b = session.getAllowLiterals();
        try {
            // need to temporarily enable it, in case we are in
            // ALLOW_LITERALS_NUMBERS mode
            session.setAllowLiterals(true);
            return prepare(session, buff.toString(), paramValues);
        } finally {
            session.setAllowLiterals(b);
        }
    }

    private static Prepared prepare(SessionLocal s, String sql,
            ArrayList<Value> paramValues) {
        Prepared prep = s.prepare(sql);
        ArrayList<Parameter> params = prep.getParameters();
        if (params != null) {
            for (int i = 0, size = params.size(); i < size; i++) {
                Parameter p = params.get(i);
                p.setValue(paramValues.get(i));
            }
        }
        return prep;
    }

    private boolean isQuery() {
        int start = lastParseIndex;
        while (readIf(OPEN_PAREN)) {
            // need to read ahead, it could be a nested union:
            // ((select 1) union (select 1))
        }
        boolean query;
        switch (currentTokenType) {
        case SELECT:
        case VALUES:
        case WITH:
            query = true;
            break;
        case TABLE:
            read();
            query = !readIf(OPEN_PAREN);
            break;
        default:
            query = false;
        }
        reread(start);
        return query;
    }

    private Prepared parseMerge(int start) {
        read("INTO");
        TableFilter targetTableFilter = readSimpleTableFilter();
        if (readIf(USING)) {
            return parseMergeUsing(targetTableFilter, start);
        }
        return parseMergeInto(targetTableFilter, start);
    }

    private Prepared parseMergeInto(TableFilter targetTableFilter, int start) {
        Merge command = new Merge(session, false);
        currentPrepared = command;
        command.setTable(targetTableFilter.getTable());
        Table table = command.getTable();
        if (readIf(OPEN_PAREN)) {
            if (isQuery()) {
                command.setQuery(parseQuery());
                read(CLOSE_PAREN);
                return command;
            }
            command.setColumns(parseColumnList(table));
        }
        if (readIf(KEY)) {
            read(OPEN_PAREN);
            command.setKeys(parseColumnList(table));
        }
        if (readIf(VALUES)) {
            parseValuesForCommand(command);
        } else {
            command.setQuery(parseQuery());
        }
        setSQL(command, start);
        return command;
    }

    private MergeUsing parseMergeUsing(TableFilter targetTableFilter, int start) {
        MergeUsing command = new MergeUsing(session, targetTableFilter);
        currentPrepared = command;

        if (isQuery()) {
            Query query = parseQuery();
            String queryAlias = readFromAlias(null);
            ArrayList<String> derivedColumnNames = null;
            if (queryAlias == null) {
                queryAlias = Constants.PREFIX_QUERY_ALIAS + parseIndex;
            } else {
                derivedColumnNames = readDerivedColumnNames();
            }

            String[] querySQLOutput = new String[1];
            List<Column> columnTemplateList = TableView.createQueryColumnTemplateList(null, query, querySQLOutput);
            TableView temporarySourceTableView = createCTEView(
                    queryAlias, querySQLOutput[0],
                    columnTemplateList, false/* no recursion */,
                    false/* do not add to session */,
                    true /* isTemporary */
            );
            TableFilter sourceTableFilter = new TableFilter(session,
                    temporarySourceTableView, queryAlias,
                    rightsChecked, null, 0, null);
            if (derivedColumnNames != null) {
                sourceTableFilter.setDerivedColumns(derivedColumnNames);
            }
            command.setSourceTableFilter(sourceTableFilter);
            if (cteCleanups == null) {
                cteCleanups = new ArrayList<>(1);
            }
            cteCleanups.add(temporarySourceTableView);
        } else {
            command.setSourceTableFilter(readTableFilter());
        }
        read(ON);
        Expression condition = readExpression();
        command.setOnCondition(condition);

        read(WHEN);
        do {
            boolean matched = readIf("MATCHED");
            if (matched) {
                parseWhenMatched(command);
            } else {
                parseWhenNotMatched(command);
            }
        } while (readIf(WHEN));

        setSQL(command, start);
        return command;
    }

    private void parseWhenMatched(MergeUsing command) {
        Expression and = readIf(AND) ? readExpression() : null;
        read("THEN");
        MergeUsing.When when;
        if (readIf("UPDATE")) {
            MergeUsing.WhenMatchedThenUpdate update = command.new WhenMatchedThenUpdate();
            update.setSetClauseList(readUpdateSetClause(command.getTargetTableFilter()));
            when = update;
        } else {
            read("DELETE");
            when = command.new WhenMatchedThenDelete();
        }
        if (and == null && database.getMode().mergeWhere && readIf(WHERE)) {
            and = readExpression();
        }
        when.setAndCondition(and);
        command.addWhen(when);
    }

    private void parseWhenNotMatched(MergeUsing command) {
        read(NOT);
        read("MATCHED");
        Expression and = readIf(AND) ? readExpression() : null;
        read("THEN");
        read("INSERT");
        Column[] columns = readIf(OPEN_PAREN) ? parseColumnList(command.getTargetTableFilter().getTable()) : null;
        Boolean overridingSystem = readIfOverriding();
        read(VALUES);
        read(OPEN_PAREN);
        ArrayList<Expression> values = Utils.newSmallArrayList();
        if (!readIf(CLOSE_PAREN)) {
            do {
                values.add(readExpressionOrDefault());
            } while (readIfMore());
        }
        MergeUsing.WhenNotMatched when = command.new WhenNotMatched(columns, overridingSystem,
                values.toArray(new Expression[0]));
        when.setAndCondition(and);
        command.addWhen(when);
    }

    private Insert parseInsert(int start) {
        Insert command = new Insert(session);
        currentPrepared = command;
        Mode mode = database.getMode();
        if (mode.onDuplicateKeyUpdate && readIf("IGNORE")) {
            command.setIgnore(true);
        }
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        Column[] columns = null;
        if (readIf(OPEN_PAREN)) {
            if (isQuery()) {
                command.setQuery(parseQuery());
                read(CLOSE_PAREN);
                return command;
            }
            columns = parseColumnList(table);
            command.setColumns(columns);
        }
        Boolean overridingSystem = readIfOverriding();
        command.setOverridingSystem(overridingSystem);
        boolean requireQuery = false;
        if (readIf("DIRECT")) {
            requireQuery = true;
            command.setInsertFromSelect(true);
        }
        if (readIf("SORTED")) {
            requireQuery = true;
        }
        readValues: {
            if (!requireQuery) {
                if (overridingSystem == null && readIf(DEFAULT)) {
                    read(VALUES);
                    command.addRow(new Expression[0]);
                    break readValues;
                }
                if (readIf(VALUES)) {
                    parseValuesForCommand(command);
                    break readValues;
                }
                if (readIf(SET)) {
                    parseInsertSet(command, table, columns);
                    break readValues;
                }
            }
            command.setQuery(parseQuery());
        }
        if (mode.onDuplicateKeyUpdate || mode.insertOnConflict || mode.isolationLevelInSelectOrInsertStatement) {
            parseInsertCompatibility(command, table, mode);
        }
        setSQL(command, start);
        return command;
    }

    private Boolean readIfOverriding() {
        Boolean overridingSystem = null;
        if (readIf("OVERRIDING")) {
            if (readIf(USER)) {
                overridingSystem = Boolean.FALSE;
            } else {
                read("SYSTEM");
                overridingSystem = Boolean.TRUE;
            }
            read(VALUE);
        }
        return overridingSystem;
    }

    private void parseInsertSet(Insert command, Table table, Column[] columns) {
        if (columns != null) {
            throw getSyntaxError();
        }
        ArrayList<Column> columnList = Utils.newSmallArrayList();
        ArrayList<Expression> values = Utils.newSmallArrayList();
        do {
            columnList.add(parseColumn(table));
            read(EQUAL);
            values.add(readExpressionOrDefault());
        } while (readIf(COMMA));
        command.setColumns(columnList.toArray(new Column[0]));
        command.addRow(values.toArray(new Expression[0]));
    }

    private void parseInsertCompatibility(Insert command, Table table, Mode mode) {
        if (mode.onDuplicateKeyUpdate) {
            if (readIf(ON)) {
                read("DUPLICATE");
                read(KEY);
                read("UPDATE");
                do {
                    String columnName = readIdentifier();
                    if (readIf(DOT)) {
                        String schemaOrTableName = columnName;
                        String tableOrColumnName = readIdentifier();
                        if (readIf(DOT)) {
                            if (!table.getSchema().getName().equals(schemaOrTableName)) {
                                throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH);
                            }
                            columnName = readIdentifier();
                        } else {
                            columnName = tableOrColumnName;
                            tableOrColumnName = schemaOrTableName;
                        }
                        if (!table.getName().equals(tableOrColumnName)) {
                            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableOrColumnName);
                        }
                    }
                    Column column = table.getColumn(columnName);
                    read(EQUAL);
                    command.addAssignmentForDuplicate(column, readExpressionOrDefault());
                } while (readIf(COMMA));
            }
        }
        if (mode.insertOnConflict) {
            if (readIf(ON)) {
                read("CONFLICT");
                read("DO");
                read("NOTHING");
                command.setIgnore(true);
            }
        }
        if (mode.isolationLevelInSelectOrInsertStatement) {
            parseIsolationClause();
        }
    }

    /**
     * MySQL compatibility. REPLACE is similar to MERGE.
     */
    private Merge parseReplace(int start) {
        Merge command = new Merge(session, true);
        currentPrepared = command;
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        if (readIf(OPEN_PAREN)) {
            if (isQuery()) {
                command.setQuery(parseQuery());
                read(CLOSE_PAREN);
                return command;
            }
            command.setColumns(parseColumnList(table));
        }
        if (readIf(VALUES)) {
            parseValuesForCommand(command);
        } else {
            command.setQuery(parseQuery());
        }
        setSQL(command, start);
        return command;
    }

    private void parseValuesForCommand(CommandWithValues command) {
        ArrayList<Expression> values = Utils.newSmallArrayList();
        do {
            values.clear();
            boolean multiColumn;
            if (readIf(ROW)) {
                read(OPEN_PAREN);
                multiColumn = true;
            } else {
                multiColumn = readIf(OPEN_PAREN);
            }
            if (multiColumn) {
                if (!readIf(CLOSE_PAREN)) {
                    do {
                        values.add(readExpressionOrDefault());
                    } while (readIfMore());
                }
            } else {
                values.add(readExpressionOrDefault());
            }
            command.addRow(values.toArray(new Expression[0]));
        } while (readIf(COMMA));
    }

    private TableFilter readTableFilter() {
        Table table;
        String alias = null;
        label: if (readIf(OPEN_PAREN)) {
            if (isQuery()) {
                return readQueryTableFilter();
            } else {
                TableFilter top;
                top = readTableFilter();
                top = readJoin(top);
                read(CLOSE_PAREN);
                alias = readFromAlias(null);
                if (alias != null) {
                    top.setAlias(alias);
                    ArrayList<String> derivedColumnNames = readDerivedColumnNames();
                    if (derivedColumnNames != null) {
                        top.setDerivedColumns(derivedColumnNames);
                    }
                }
                return top;
            }
        } else if (readIf(VALUES)) {
            TableValueConstructor query = parseValues();
            alias = session.getNextSystemIdentifier(sqlCommand);
            table = query.toTable(alias, null, parameters, createView != null, currentSelect);
        } else if (readIf(TABLE)) {
            read(OPEN_PAREN);
            ArrayTableFunction function = readTableFunction(ArrayTableFunction.TABLE);
            table = new FunctionTable(database.getMainSchema(), session, function);
        } else {
            boolean quoted = currentTokenQuoted;
            String tableName = readIdentifier();
            int backupIndex = parseIndex;
            schemaName = null;
            if (readIf(DOT)) {
                tableName = readIdentifierWithSchema2(tableName);
            } else if (!quoted && readIf(TABLE)) {
                table = readDataChangeDeltaTable(upperName(tableName), backupIndex);
                break label;
            }
            Schema schema;
            if (schemaName == null) {
                schema = null;
            } else {
                schema = findSchema(schemaName);
                if (schema == null) {
                    if (isDualTable(tableName)) {
                        table = new DualTable(database);
                        break label;
                    }
                    throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
                }
            }
            boolean foundLeftParen = readIf(OPEN_PAREN);
            if (foundLeftParen && readIf("INDEX")) {
                // Sybase compatibility with
                // "select * from test (index table1_index)"
                readIdentifierWithSchema(null);
                read(CLOSE_PAREN);
                foundLeftParen = false;
            }
            if (foundLeftParen) {
                Schema mainSchema = database.getMainSchema();
                if (equalsToken(tableName, RangeTable.NAME)
                        || equalsToken(tableName, RangeTable.ALIAS)) {
                    Expression min = readExpression();
                    read(COMMA);
                    Expression max = readExpression();
                    if (readIf(COMMA)) {
                        Expression step = readExpression();
                        read(CLOSE_PAREN);
                        table = new RangeTable(mainSchema, min, max, step);
                    } else {
                        read(CLOSE_PAREN);
                        table = new RangeTable(mainSchema, min, max);
                    }
                } else {
                    table = new FunctionTable(mainSchema, session, readTableFunction(tableName, schema));
                }
            } else {
                table = readTableOrView(tableName);
            }
        }
        ArrayList<String> derivedColumnNames = null;
        IndexHints indexHints = null;
        if (readIfUseIndex()) {
            indexHints = parseIndexHints(table);
        } else {
            alias = readFromAlias(alias);
            if (alias != null) {
                derivedColumnNames = readDerivedColumnNames();
                if (readIfUseIndex()) {
                    indexHints = parseIndexHints(table);
                }
            }
        }
        return buildTableFilter(table, alias, derivedColumnNames, indexHints);
    }

    private TableFilter readQueryTableFilter() {
        Query query = parseSelectUnion();
        read(CLOSE_PAREN);
        Table table;
        String alias;
        ArrayList<String> derivedColumnNames = null;
        IndexHints indexHints = null;
        if (readIfUseIndex()) {
            alias = session.getNextSystemIdentifier(sqlCommand);
            table = query.toTable(alias, null, parameters, createView != null, currentSelect);
            indexHints = parseIndexHints(table);
        } else {
            alias = readFromAlias(null);
            if (alias != null) {
                derivedColumnNames = readDerivedColumnNames();
                Column[] columnTemplates = null;
                if (derivedColumnNames != null) {
                    query.init();
                    columnTemplates = TableView.createQueryColumnTemplateList(
                            derivedColumnNames.toArray(new String[0]), query, new String[1])
                            .toArray(new Column[0]);
                }
                table = query.toTable(alias, columnTemplates, parameters, createView != null, currentSelect);
                if (readIfUseIndex()) {
                    indexHints = parseIndexHints(table);
                }
            } else {
                alias = session.getNextSystemIdentifier(sqlCommand);
                table = query.toTable(alias, null, parameters, createView != null, currentSelect);
            }
        }
        return buildTableFilter(table, alias, derivedColumnNames, indexHints);
    }

    private TableFilter buildTableFilter(Table table, String alias, ArrayList<String> derivedColumnNames,
            IndexHints indexHints) {
        if (database.getMode().discardWithTableHints) {
            discardWithTableHints();
        }
        // inherit alias for CTE as views from table name
        if (alias == null && table.isView() && table.isTableExpression()) {
            alias = table.getName();
        }
        TableFilter filter = new TableFilter(session, table, alias, rightsChecked,
                currentSelect, orderInFrom++, indexHints);
        if (derivedColumnNames != null) {
            filter.setDerivedColumns(derivedColumnNames);
        }
        return filter;
    }

    private Table readDataChangeDeltaTable(String resultOptionName, int backupIndex) {
        read(OPEN_PAREN);
        int start = lastParseIndex;
        DataChangeStatement statement;
        ResultOption resultOption = ResultOption.FINAL;
        switch (resultOptionName) {
        case "OLD":
            resultOption = ResultOption.OLD;
            if (readIf("UPDATE")) {
                statement = parseUpdate(start);
            } else if (readIf("DELETE")) {
                statement = parseDelete(start);
            } else if (readIf("MERGE")) {
                statement = (DataChangeStatement) parseMerge(start);
            } else if (database.getMode().replaceInto && readIf("REPLACE")) {
                statement = parseReplace(start);
            } else {
                throw getSyntaxError();
            }
            break;
        case "NEW":
            resultOption = ResultOption.NEW;
            //$FALL-THROUGH$
        case "FINAL":
            if (readIf("INSERT")) {
                statement = parseInsert(start);
            } else if (readIf("UPDATE")) {
                statement = parseUpdate(start);
            } else if (readIf("MERGE")) {
                statement = (DataChangeStatement) parseMerge(start);
            } else if (database.getMode().replaceInto && readIf("REPLACE")) {
                statement = parseReplace(start);
            } else {
                throw getSyntaxError();
            }
            break;
        default:
            parseIndex = backupIndex;
            addExpected("OLD TABLE");
            addExpected("NEW TABLE");
            addExpected("FINAL TABLE");
            throw getSyntaxError();
        }
        read(CLOSE_PAREN);
        return new DataChangeDeltaTable(getSchemaWithDefault(), session, statement, resultOption);
    }

    private TableFunction readTableFunction(String name, Schema schema) {
        if (schema == null) {
            switch (upperName(name)) {
            case "UNNEST":
                return readUnnestFunction();
            case "TABLE_DISTINCT":
                return readTableFunction(ArrayTableFunction.TABLE_DISTINCT);
            case "CSVREAD":
                recompileAlways = true;
                return readParameters(new CSVReadFunction());
            case "LINK_SCHEMA":
                recompileAlways = true;
                return readParameters(new LinkSchemaFunction());
            }
        }
        FunctionAlias functionAlias = getFunctionAliasWithinPath(name, schema);
        if (!functionAlias.isDeterministic()) {
            recompileAlways = true;
        }
        ArrayList<Expression> argList = Utils.newSmallArrayList();
        if (!readIf(CLOSE_PAREN)) {
            do {
                argList.add(readExpression());
            } while (readIfMore());
        }
        return new JavaTableFunction(functionAlias, argList.toArray(new Expression[0]));
    }

    private boolean readIfUseIndex() {
        int start = lastParseIndex;
        if (!readIf("USE")) {
            return false;
        }
        if (!readIf("INDEX")) {
            reread(start);
            return false;
        }
        return true;
    }

    private IndexHints parseIndexHints(Table table) {
        read(OPEN_PAREN);
        LinkedHashSet<String> indexNames = new LinkedHashSet<>();
        if (!readIf(CLOSE_PAREN)) {
            do {
                String indexName = readIdentifierWithSchema();
                Index index = table.getIndex(indexName);
                indexNames.add(index.getName());
            } while (readIfMore());
        }
        return IndexHints.createUseIndexHints(indexNames);
    }

    private String readFromAlias(String alias) {
        if (readIf(AS) || isIdentifier()) {
            alias = readIdentifier();
        }
        return alias;
    }

    private ArrayList<String> readDerivedColumnNames() {
        if (readIf(OPEN_PAREN)) {
            ArrayList<String> derivedColumnNames = new ArrayList<>();
            do {
                derivedColumnNames.add(readIdentifier());
            } while (readIfMore());
            return derivedColumnNames;
        }
        return null;
    }

    private void discardWithTableHints() {
        if (readIf(WITH)) {
            read(OPEN_PAREN);
            do {
                discardTableHint();
            } while (readIfMore());
        }
    }

    private void discardTableHint() {
        if (readIf("INDEX")) {
            if (readIf(OPEN_PAREN)) {
                do {
                    readExpression();
                } while (readIfMore());
            } else {
                read(EQUAL);
                readExpression();
            }
        } else {
            readExpression();
        }
    }

    private Prepared parseTruncate() {
        read(TABLE);
        Table table = readTableOrView();
        boolean restart = database.getMode().truncateTableRestartIdentity;
        if (readIf("CONTINUE")) {
            read("IDENTITY");
            restart = false;
        } else if (readIf("RESTART")) {
            read("IDENTITY");
            restart = true;
        }
        TruncateTable command = new TruncateTable(session);
        command.setTable(table);
        command.setRestart(restart);
        return command;
    }

    private boolean readIfExists(boolean ifExists) {
        if (readIf(IF)) {
            read(EXISTS);
            ifExists = true;
        }
        return ifExists;
    }

    private Prepared parseComment() {
        int type = 0;
        read(ON);
        boolean column = false;
        if (readIf(TABLE) || readIf("VIEW")) {
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("COLUMN")) {
            column = true;
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("CONSTANT")) {
            type = DbObject.CONSTANT;
        } else if (readIf(CONSTRAINT)) {
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
        } else if (readIf(USER)) {
            type = DbObject.USER;
        } else if (readIf("DOMAIN")) {
            type = DbObject.DOMAIN;
        } else {
            throw getSyntaxError();
        }
        SetComment command = new SetComment(session);
        String objectName;
        if (column) {
            // can't use readIdentifierWithSchema() because
            // it would not read [catalog.]schema.table.column correctly
            objectName = readIdentifier();
            String tmpSchemaName = null;
            read(DOT);
            boolean allowEmpty = database.getMode().allowEmptySchemaValuesAsDefaultSchema;
            String columnName = allowEmpty && currentTokenType == DOT ? null : readIdentifier();
            if (readIf(DOT)) {
                tmpSchemaName = objectName;
                objectName = columnName;
                columnName = allowEmpty && currentTokenType == DOT ? null : readIdentifier();
                if (readIf(DOT)) {
                    checkDatabaseName(tmpSchemaName);
                    tmpSchemaName = objectName;
                    objectName = columnName;
                    columnName = readIdentifier();
                }
            }
            if (columnName == null || objectName == null) {
                throw DbException.getSyntaxError(sqlCommand, lastParseIndex, "table.column");
            }
            schemaName = tmpSchemaName != null ? tmpSchemaName : session.getCurrentSchemaName();
            command.setColumn(true);
            command.setColumnName(columnName);
        } else {
            objectName = readIdentifierWithSchema();
        }
        command.setSchemaName(schemaName);
        command.setObjectName(objectName);
        command.setObjectType(type);
        read(IS);
        command.setCommentExpression(readExpression());
        return command;
    }

    private Prepared parseDrop() {
        if (readIf(TABLE)) {
            boolean ifExists = readIfExists(false);
            DropTable command = new DropTable(session);
            do {
                String tableName = readIdentifierWithSchema();
                command.addTable(getSchema(), tableName);
            } while (readIf(COMMA));
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            if (readIf("CASCADE")) {
                command.setDropAction(ConstraintActionType.CASCADE);
                readIf("CONSTRAINTS");
            } else if (readIf("RESTRICT")) {
                command.setDropAction(ConstraintActionType.RESTRICT);
            } else if (readIf("IGNORE")) {
                // TODO SET_DEFAULT works in the same way as CASCADE
                command.setDropAction(ConstraintActionType.SET_DEFAULT);
            }
            return command;
        } else if (readIf("INDEX")) {
            boolean ifExists = readIfExists(false);
            String indexName = readIdentifierWithSchema();
            DropIndex command = new DropIndex(session, getSchema());
            command.setIndexName(indexName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            //Support for MySQL: DROP INDEX index_name ON tbl_name
            if (readIf(ON)) {
                readIdentifierWithSchema();
            }
            return command;
        } else if (readIf(USER)) {
            boolean ifExists = readIfExists(false);
            DropUser command = new DropUser(session);
            command.setUserName(readIdentifier());
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
            ConstraintActionType dropAction = parseCascadeOrRestrict();
            if (dropAction != null) {
                command.setDropAction(dropAction);
            }
            return command;
        } else if (readIf("ROLE")) {
            boolean ifExists = readIfExists(false);
            DropRole command = new DropRole(session);
            command.setRoleName(readIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("ALIAS")) {
            boolean ifExists = readIfExists(false);
            String aliasName = readIdentifierWithSchema();
            DropFunctionAlias command = new DropFunctionAlias(session,
                    getSchema());
            command.setAliasName(aliasName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("SCHEMA")) {
            boolean ifExists = readIfExists(false);
            DropSchema command = new DropSchema(session);
            command.setSchemaName(readIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            ConstraintActionType dropAction = parseCascadeOrRestrict();
            if (dropAction != null) {
                command.setDropAction(dropAction);
            }
            return command;
        } else if (readIf(ALL)) {
            read("OBJECTS");
            DropDatabase command = new DropDatabase(session);
            command.setDropAllObjects(true);
            if (readIf("DELETE")) {
                read("FILES");
                command.setDeleteFiles(true);
            }
            return command;
        } else if (readIf("DOMAIN") || readIf("TYPE") || readIf("DATATYPE")) {
            return parseDropDomain();
        } else if (readIf("AGGREGATE")) {
            return parseDropAggregate();
        } else if (readIf("SYNONYM")) {
            boolean ifExists = readIfExists(false);
            String synonymName = readIdentifierWithSchema();
            DropSynonym command = new DropSynonym(session, getSchema());
            command.setSynonymName(synonymName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        throw getSyntaxError();
    }

    private DropDomain parseDropDomain() {
        boolean ifExists = readIfExists(false);
        String domainName = readIdentifierWithSchema();
        DropDomain command = new DropDomain(session, getSchema());
        command.setDomainName(domainName);
        ifExists = readIfExists(ifExists);
        command.setIfDomainExists(ifExists);
        ConstraintActionType dropAction = parseCascadeOrRestrict();
        if (dropAction != null) {
            command.setDropAction(dropAction);
        }
        return command;
    }

    private DropAggregate parseDropAggregate() {
        boolean ifExists = readIfExists(false);
        String name = readIdentifierWithSchema();
        DropAggregate command = new DropAggregate(session, getSchema());
        command.setName(name);
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private TableFilter readJoin(TableFilter top) {
        for (TableFilter last = top, join;; last = join) {
            switch (currentTokenType) {
            case RIGHT: {
                read();
                readIf("OUTER");
                read(JOIN);
                // the right hand side is the 'inner' table usually
                join = readTableFilter();
                join = readJoin(join);
                Expression on = readJoinSpecification(top, join, true);
                addJoin(join, top, true, on);
                top = join;
                break;
            }
            case LEFT: {
                read();
                readIf("OUTER");
                read(JOIN);
                join = readTableFilter();
                join = readJoin(join);
                Expression on = readJoinSpecification(top, join, false);
                addJoin(top, join, true, on);
                break;
            }
            case FULL:
                read();
                throw getSyntaxError();
            case INNER: {
                read();
                read(JOIN);
                join = readTableFilter();
                top = readJoin(top);
                Expression on = readJoinSpecification(top, join, false);
                addJoin(top, join, false, on);
                break;
            }
            case JOIN: {
                read();
                join = readTableFilter();
                top = readJoin(top);
                Expression on = readJoinSpecification(top, join, false);
                addJoin(top, join, false, on);
                break;
            }
            case CROSS: {
                read();
                read(JOIN);
                join = readTableFilter();
                addJoin(top, join, false, null);
                break;
            }
            case NATURAL: {
                read();
                read(JOIN);
                join = readTableFilter();
                Expression on = null;
                for (Column column1 : last.getTable().getColumns()) {
                    Column column2 = join.getColumn(last.getColumnName(column1), true);
                    if (column2 != null) {
                        on = addJoinColumn(on, last, join, column1, column2, false);
                    }
                }
                addJoin(top, join, false, on);
                break;
            }
            default:
                if (expectedList != null) {
                    // FULL is intentionally excluded
                    addMultipleExpected(RIGHT, LEFT, INNER, JOIN, CROSS, NATURAL);
                }
                return top;
            }
        }
    }

    private Expression readJoinSpecification(TableFilter filter1, TableFilter filter2, boolean rightJoin) {
        Expression on = null;
        if (readIf(ON)) {
            on = readExpression();
        } else if (readIf(USING)) {
            read(OPEN_PAREN);
            do {
                String columnName = readIdentifier();
                on = addJoinColumn(on, filter1, filter2, filter1.getColumn(columnName, false),
                        filter2.getColumn(columnName, false), rightJoin);
            } while (readIfMore());
        }
        return on;
    }

    private Expression addJoinColumn(Expression on, TableFilter filter1, TableFilter filter2, Column column1,
            Column column2, boolean rightJoin) {
        if (rightJoin) {
            filter1.addCommonJoinColumns(column1, column2, filter2);
            filter2.addCommonJoinColumnToExclude(column2);
        } else {
            filter1.addCommonJoinColumns(column1, column1, filter1);
            filter2.addCommonJoinColumnToExclude(column2);
        }
        Expression tableExpr = new ExpressionColumn(database, filter1.getSchemaName(), filter1.getTableAlias(),
                filter1.getColumnName(column1));
        Expression joinExpr = new ExpressionColumn(database, filter2.getSchemaName(), filter2.getTableAlias(),
                filter2.getColumnName(column2));
        Expression equal = new Comparison(Comparison.EQUAL, tableExpr, joinExpr, false);
        if (on == null) {
            on = equal;
        } else {
            on = new ConditionAndOr(ConditionAndOr.AND, on, equal);
        }
        return on;
    }

    /**
     * Add one join to another. This method creates nested join between them if
     * required.
     *
     * @param top parent join
     * @param join child join
     * @param outer if child join is an outer join
     * @param on the join condition
     * @see TableFilter#addJoin(TableFilter, boolean, Expression)
     */
    private void addJoin(TableFilter top, TableFilter join, boolean outer, Expression on) {
        if (join.getJoin() != null) {
            String joinTable = Constants.PREFIX_JOIN + parseIndex;
            TableFilter n = new TableFilter(session, new DualTable(database),
                    joinTable, rightsChecked, currentSelect, join.getOrderInFrom(),
                    null);
            n.setNestedJoin(join);
            join = n;
        }
        top.addJoin(join, outer, on);
    }

    private Prepared parseExecutePostgre() {
        if (readIf("IMMEDIATE")) {
            return new ExecuteImmediate(session, readExpression());
        }
        ExecuteProcedure command = new ExecuteProcedure(session);
        String procedureName = readIdentifier();
        Procedure p = session.getProcedure(procedureName);
        if (p == null) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1,
                    procedureName);
        }
        command.setProcedure(p);
        if (readIf(OPEN_PAREN)) {
            for (int i = 0;; i++) {
                command.setExpression(i, readExpression());
                if (!readIfMore()) {
                    break;
                }
            }
        }
        return command;
    }

    private Prepared parseExecuteSQLServer() {
        Call command = new Call(session);
        currentPrepared = command;
        String schemaName = null;
        String name = readIdentifier();
        if (readIf(DOT)) {
            schemaName = name;
            name = readIdentifier();
            if (readIf(DOT)) {
                checkDatabaseName(schemaName);
                schemaName = name;
                name = readIdentifier();
            }
        }
        FunctionAlias functionAlias = getFunctionAliasWithinPath(name,
                schemaName != null ? database.getSchema(schemaName) : null);
        Expression[] args;
        ArrayList<Expression> argList = Utils.newSmallArrayList();
        if (currentTokenType != SEMICOLON && currentTokenType != END_OF_INPUT) {
            do {
                argList.add(readExpression());
            } while (readIf(COMMA));
        }
        args = argList.toArray(new Expression[0]);
        command.setExpression(new JavaFunction(functionAlias, args));
        return command;
    }

    private FunctionAlias getFunctionAliasWithinPath(String name, Schema schema) {
        UserDefinedFunction userDefinedFunction = findUserDefinedFunctionWithinPath(schema, name);
        if (userDefinedFunction instanceof FunctionAlias) {
            return (FunctionAlias) userDefinedFunction;
        }
        throw DbException.get(ErrorCode.FUNCTION_NOT_FOUND_1, name);
    }

    private DeallocateProcedure parseDeallocate() {
        readIf("PLAN");
        DeallocateProcedure command = new DeallocateProcedure(session);
        command.setProcedureName(readIdentifier());
        return command;
    }

    private Explain parseExplain() {
        Explain command = new Explain(session);
        if (readIf("ANALYZE")) {
            command.setExecuteCommand(true);
        } else {
            if (readIf("PLAN")) {
                readIf(FOR);
            }
        }
        switch (currentTokenType) {
        case SELECT:
        case TABLE:
        case VALUES:
        case WITH:
        case OPEN_PAREN:
            Query query = parseQuery();
            query.setNeverLazy(true);
            command.setCommand(query);
            break;
        default:
            int start = lastParseIndex;
            if (readIf("DELETE")) {
                command.setCommand(parseDelete(start));
            } else if (readIf("UPDATE")) {
                command.setCommand(parseUpdate(start));
            } else if (readIf("INSERT")) {
                command.setCommand(parseInsert(start));
            } else if (readIf("MERGE")) {
                command.setCommand(parseMerge(start));
            } else {
                throw getSyntaxError();
            }
        }
        return command;
    }

    private Query parseQuery() {
        int paramIndex = parameters.size();
        Query command = parseSelectUnion();
        int size = parameters.size();
        ArrayList<Parameter> params = new ArrayList<>(size);
        for (int i = paramIndex; i < size; i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        command.init();
        return command;
    }

    private Prepared parseWithStatementOrQuery(int start) {
        int paramIndex = parameters.size();
        Prepared command = parseWith();
        int size = parameters.size();
        ArrayList<Parameter> params = new ArrayList<>(size);
        for (int i = paramIndex; i < size; i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        if (command instanceof Query) {
            Query query = (Query) command;
            query.init();
        }
        setSQL(command, start);
        return command;
    }

    private Query parseSelectUnion() {
        int start = lastParseIndex;
        Query command = parseQuerySub();
        for (;;) {
            SelectUnion.UnionType type;
            if (readIf(UNION)) {
                if (readIf(ALL)) {
                    type = SelectUnion.UnionType.UNION_ALL;
                } else {
                    readIf(DISTINCT);
                    type = SelectUnion.UnionType.UNION;
                }
            } else if (readIf(EXCEPT) || readIf(MINUS)) {
                type = SelectUnion.UnionType.EXCEPT;
            } else if (readIf(INTERSECT)) {
                type = SelectUnion.UnionType.INTERSECT;
            } else {
                break;
            }
            command = new SelectUnion(session, type, command, parseQuerySub());
        }
        parseEndOfQuery(command);
        setSQL(command, start);
        return command;
    }

    private void parseEndOfQuery(Query command) {
        if (readIf(ORDER)) {
            read("BY");
            Select oldSelect = currentSelect;
            if (command instanceof Select) {
                currentSelect = (Select) command;
            }
            ArrayList<QueryOrderBy> orderList = Utils.newSmallArrayList();
            do {
                boolean canBeNumber = !readIf(EQUAL);
                QueryOrderBy order = new QueryOrderBy();
                Expression expr = readExpression();
                if (canBeNumber && expr instanceof ValueExpression && expr.getType().getValueType() == Value.INTEGER) {
                    order.columnIndexExpr = expr;
                } else if (expr instanceof Parameter) {
                    recompileAlways = true;
                    order.columnIndexExpr = expr;
                } else {
                    order.expression = expr;
                }
                order.sortType = parseSortType();
                orderList.add(order);
            } while (readIf(COMMA));
            command.setOrder(orderList);
            currentSelect = oldSelect;
        }
        if (command.getFetch() == null) {
            // make sure aggregate functions will not work here
            Select temp = currentSelect;
            currentSelect = null;
            boolean hasOffsetOrFetch = false;
            // Standard SQL OFFSET / FETCH
            if (readIf(OFFSET)) {
                hasOffsetOrFetch = true;
                command.setOffset(readExpression().optimize(session));
                if (!readIf(ROW)) {
                    readIf("ROWS");
                }
            }
            if (readIf(FETCH)) {
                hasOffsetOrFetch = true;
                if (!readIf("FIRST")) {
                    read("NEXT");
                }
                if (readIf(ROW) || readIf("ROWS")) {
                    command.setFetch(ValueExpression.get(ValueInteger.get(1)));
                } else {
                    command.setFetch(readExpression().optimize(session));
                    if (readIf("PERCENT")) {
                        command.setFetchPercent(true);
                    }
                    if (!readIf(ROW)) {
                        read("ROWS");
                    }
                }
                if (readIf(WITH)) {
                    read("TIES");
                    command.setWithTies(true);
                } else {
                    read("ONLY");
                }
            }
            // MySQL-style LIMIT / OFFSET
            if (!hasOffsetOrFetch && database.getMode().limit && readIf(LIMIT)) {
                Expression limit = readExpression().optimize(session);
                if (readIf(OFFSET)) {
                    command.setOffset(readExpression().optimize(session));
                } else if (readIf(COMMA)) {
                    // MySQL: [offset, ] rowcount
                    Expression offset = limit;
                    limit = readExpression().optimize(session);
                    command.setOffset(offset);
                }
                command.setFetch(limit);
            }
            currentSelect = temp;
        }
        if (readIf(FOR)) {
            if (readIf("UPDATE")) {
                if (readIf("OF")) {
                    do {
                        readIdentifierWithSchema();
                    } while (readIf(COMMA));
                } else if (readIf("NOWAIT")) {
                    // TODO parser: select for update nowait: should not wait
                }
                command.setForUpdate(true);
            } else if (readIf("READ") || readIf(FETCH)) {
                read("ONLY");
            }
        }
        if (database.getMode().isolationLevelInSelectOrInsertStatement) {
            parseIsolationClause();
        }
    }

    /**
     * DB2 isolation clause
     */
    private void parseIsolationClause() {
        if (readIf(WITH)) {
            if (readIf("RR") || readIf("RS")) {
                // concurrent-access-resolution clause
                if (readIf("USE")) {
                    read(AND);
                    read("KEEP");
                    if (readIf("SHARE") || readIf("UPDATE") ||
                            readIf("EXCLUSIVE")) {
                        // ignore
                    }
                    read("LOCKS");
                }
            } else if (readIf("CS") || readIf("UR")) {
                // ignore
            }
        }
    }

    private Query parseQuerySub() {
        if (readIf(OPEN_PAREN)) {
            Query command = parseSelectUnion();
            read(CLOSE_PAREN);
            return command;
        }
        if (readIf(WITH)) {
            Query query;
            try {
                query = (Query) parseWith();
            } catch (ClassCastException e) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                        "WITH statement supports only SELECT (query) in this context");
            }
            // recursive can not be lazy
            query.setNeverLazy(true);
            return query;
        }
        int start = lastParseIndex;
        if (readIf(SELECT)) {
            return parseSelect(start);
        } else if (readIf(TABLE)) {
            return parseExplicitTable(start);
        }
        read(VALUES);
        return parseValues();
    }

    private void parseSelectFromPart(Select command) {
        do {
            TableFilter filter = readTableFilter();
            parseJoinTableFilter(filter, command);
        } while (readIf(COMMA));

        // Parser can reorder joined table filters, need to explicitly sort them
        // to get the order as it was in the original query.
        if (session.isForceJoinOrder()) {
            command.getTopFilters().sort(TABLE_FILTER_COMPARATOR);
        }
    }

    private void parseJoinTableFilter(TableFilter top, final Select command) {
        top = readJoin(top);
        command.addTableFilter(top, true);
        boolean isOuter = false;
        while (true) {
            TableFilter n = top.getNestedJoin();
            if (n != null) {
                n.visit(f -> command.addTableFilter(f, false));
            }
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

    private void parseSelectExpressions(Select command) {
        Select temp = currentSelect;
        // make sure aggregate functions will not work in TOP and LIMIT
        currentSelect = null;
        if (database.getMode().topInSelect && readIf("TOP")) {
            // can't read more complex expressions here because
            // SELECT TOP 1 +? A FROM TEST could mean
            // SELECT TOP (1+?) A FROM TEST or
            // SELECT TOP 1 (+?) AS A FROM TEST
            command.setFetch(readTerm().optimize(session));
            if (readIf("PERCENT")) {
                command.setFetchPercent(true);
            }
            if (readIf(WITH)) {
                read("TIES");
                command.setWithTies(true);
            }
        }
        currentSelect = temp;
        if (readIf(DISTINCT)) {
            if (readIf(ON)) {
                read(OPEN_PAREN);
                ArrayList<Expression> distinctExpressions = Utils.newSmallArrayList();
                do {
                    distinctExpressions.add(readExpression());
                } while (readIfMore());
                command.setDistinct(distinctExpressions.toArray(new Expression[0]));
            } else {
                command.setDistinct();
            }
        } else {
            readIf(ALL);
        }
        ArrayList<Expression> expressions = Utils.newSmallArrayList();
        do {
            if (readIf(ASTERISK)) {
                expressions.add(parseWildcard(null, null));
            } else {
                switch (currentTokenType) {
                case FROM:
                case WHERE:
                case GROUP:
                case HAVING:
                case WINDOW:
                case QUALIFY:
                case ORDER:
                case OFFSET:
                case FETCH:
                case CLOSE_PAREN:
                case SEMICOLON:
                case END_OF_INPUT:
                    break;
                default:
                    Expression expr = readExpression();
                    if (readIf(AS) || isIdentifier()) {
                        expr = new Alias(expr, readIdentifier(), database.getMode().aliasColumnName);
                    }
                    expressions.add(expr);
                }
            }
        } while (readIf(COMMA));
        command.setExpressions(expressions);
    }

    private Select parseSelect(int start) {
        Select command = new Select(session, currentSelect);
        Select oldSelect = currentSelect;
        Prepared oldPrepared = currentPrepared;
        currentSelect = command;
        currentPrepared = command;
        parseSelectExpressions(command);
        if (!readIf(FROM)) {
            // select without FROM
            TableFilter filter = new TableFilter(session, new DualTable(database), null, rightsChecked,
                    currentSelect, 0, null);
            command.addTableFilter(filter, true);
        } else {
            parseSelectFromPart(command);
        }
        if (readIf(WHERE)) {
            command.addCondition(readExpressionWithGlobalConditions());
        }
        // the group by is read for the outer select (or not a select)
        // so that columns that are not grouped can be used
        currentSelect = oldSelect;
        if (readIf(GROUP)) {
            read("BY");
            command.setGroupQuery();
            ArrayList<Expression> list = Utils.newSmallArrayList();
            do {
                if (isToken(OPEN_PAREN) && isOrdinaryGroupingSet()) {
                    if (!readIf(CLOSE_PAREN)) {
                        do {
                            list.add(readExpression());
                        } while (readIfMore());
                    }
                } else {
                    Expression expr = readExpression();
                    if (database.getMode().groupByColumnIndex && expr instanceof ValueExpression &&
                            expr.getType().getValueType() == Value.INTEGER) {
                        ArrayList<Expression> expressions = command.getExpressions();
                        for (Expression e : expressions) {
                            if (e instanceof Wildcard) {
                                throw getSyntaxError();
                            }
                        }
                        int idx = expr.getValue(session).getInt();
                        if (idx < 1 || idx > expressions.size()) {
                            throw DbException.get(ErrorCode.GROUP_BY_NOT_IN_THE_RESULT, Integer.toString(idx),
                                    Integer.toString(expressions.size()));
                        }
                        list.add(expressions.get(idx-1));
                    } else {
                        list.add(expr);
                    }
                }
            } while (readIf(COMMA));
            if (!list.isEmpty()) {
                command.setGroupBy(list);
            }
        }
        currentSelect = command;
        if (readIf(HAVING)) {
            command.setGroupQuery();
            command.setHaving(readExpressionWithGlobalConditions());
        }
        if (readIf(WINDOW)) {
            do {
                int index = parseIndex;
                String name = readIdentifier();
                read(AS);
                Window w = readWindowSpecification();
                if (!currentSelect.addWindow(name, w)) {
                    throw DbException.getSyntaxError(sqlCommand, index, "unique identifier");
                }
            } while (readIf(COMMA));
        }
        if (readIf(QUALIFY)) {
            command.setWindowQuery();
            command.setQualify(readExpressionWithGlobalConditions());
        }
        command.setParameterList(parameters);
        currentSelect = oldSelect;
        currentPrepared = oldPrepared;
        setSQL(command, start);
        return command;
    }

    /**
     * Checks whether current opening parenthesis can be a start of ordinary
     * grouping set. This method reads this parenthesis if it is.
     *
     * @return whether current opening parenthesis can be a start of ordinary
     *         grouping set
     */
    private boolean isOrdinaryGroupingSet() {
        int lastIndex = lastParseIndex, index = parseIndex;
        int level = 1;
        loop: for (;;) {
            read();
            switch (currentTokenType) {
            case CLOSE_PAREN:
                if (--level <= 0) {
                    break loop;
                }
                break;
            case OPEN_PAREN:
                level++;
                break;
            case END_OF_INPUT:
                addExpected(CLOSE_PAREN);
                throw getSyntaxError();
            }
        }
        read();
        switch (currentTokenType) {
        // End of query
        case CLOSE_PAREN:
        case SEMICOLON:
        case END_OF_INPUT:
        // Next grouping element
        case COMMA:
        // Next select clause
        case HAVING:
        case WINDOW:
        case QUALIFY:
        // Next query expression body clause
        case UNION:
        case EXCEPT:
        case MINUS:
        case INTERSECT:
        // Next query expression clause
        case ORDER:
        case OFFSET:
        case FETCH:
        case LIMIT:
        case FOR:
            reread(index);
            return true;
        default:
            reread(lastIndex);
            return false;
        }
    }

    private Query parseExplicitTable(int start) {
        Table table = readTableOrView();
        Select command = new Select(session, currentSelect);
        TableFilter filter = new TableFilter(session, table, null, rightsChecked,
                command, orderInFrom++, null);
        command.addTableFilter(filter, true);
        command.setExplicitTable();
        setSQL(command, start);
        return command;
    }

    private void setSQL(Prepared command, int startIndex) {
        command.setSQL(StringUtils.trimSubstring(originalSQL, startIndex, lastParseIndex));
    }

    private Expression readExpressionOrDefault() {
        if (readIf(DEFAULT)) {
            return ValueExpression.DEFAULT;
        }
        return readExpression();
    }

    private Expression readExpressionWithGlobalConditions() {
        Expression r = readCondition();
        if (readIf(AND)) {
            r = readAnd(new ConditionAndOr(ConditionAndOr.AND, r, readCondition()));
        } else if (readIf("_LOCAL_AND_GLOBAL_")) {
            r = readAnd(new ConditionLocalAndGlobal(r, readCondition()));
        }
        return readExpressionPart2(r);
    }

    private Expression readExpression() {
        return readExpressionPart2(readAnd(readCondition()));
    }

    private Expression readExpressionPart2(Expression r1) {
        if (!readIf(OR)) {
            return r1;
        }
        Expression r2 = readAnd(readCondition());
        if (!readIf(OR)) {
            return new ConditionAndOr(ConditionAndOr.OR, r1, r2);
        }
        // Above logic to avoid allocating an ArrayList for the common case.
        // We combine into ConditionAndOrN here rather than letting the optimisation
        // pass do it, to avoid StackOverflowError during stuff like mapColumns.
        final ArrayList<Expression> expressions = new ArrayList<>();
        expressions.add(r1);
        expressions.add(r2);
        do {
            expressions.add(readAnd(readCondition()));
        }
        while (readIf(OR));
        return new ConditionAndOrN(ConditionAndOr.OR, expressions);
    }

    private Expression readAnd(Expression r) {
        if (!readIf(AND)) {
            return r;
        }
        Expression expr2 = readCondition();
        if (!readIf(AND)) {
            return new ConditionAndOr(ConditionAndOr.AND, r, expr2);
        }
        // Above logic to avoid allocating an ArrayList for the common case.
        // We combine into ConditionAndOrN here rather than letting the optimisation
        // pass do it, to avoid StackOverflowError during stuff like mapColumns.
        final ArrayList<Expression> expressions = new ArrayList<>();
        expressions.add(r);
        expressions.add(expr2);
        do {
            expressions.add(readCondition());
        }
        while (readIf(AND));
        return new ConditionAndOrN(ConditionAndOr.AND, expressions);
    }

    private Expression readCondition() {
        switch (currentTokenType) {
        case NOT:
            read();
            return new ConditionNot(readCondition());
        case EXISTS: {
            read();
            read(OPEN_PAREN);
            Query query = parseQuery();
            // can not reduce expression because it might be a union except
            // query with distinct
            read(CLOSE_PAREN);
            return new ExistsPredicate(query);
        }
        case INTERSECTS: {
            read();
            read(OPEN_PAREN);
            Expression r1 = readConcat();
            read(COMMA);
            Expression r2 = readConcat();
            read(CLOSE_PAREN);
            return new Comparison(Comparison.SPATIAL_INTERSECTS, r1, r2, false);
        }
        case UNIQUE: {
            read();
            read(OPEN_PAREN);
            Query query = parseQuery();
            read(CLOSE_PAREN);
            return new UniquePredicate(query);
        }
        default:
            if (expectedList != null) {
                addMultipleExpected(NOT, EXISTS, INTERSECTS, UNIQUE);
            }
        }
        Expression l, c = readConcat();
        do {
            l = c;
            // special case: NOT NULL is not part of an expression (as in CREATE
            // TABLE TEST(ID INT DEFAULT 0 NOT NULL))
            int backup = parseIndex;
            boolean not = readIf(NOT);
            if (not && isToken(NULL)) {
                // this really only works for NOT NULL!
                parseIndex = backup;
                currentToken = "NOT";
                currentTokenType = NOT;
                break;
            }
            c = readConditionRightHandSide(l, not, false);
        } while (c != null);
        return l;
    }

    private Expression readConditionRightHandSide(Expression r, boolean not, boolean whenOperand) {
        if (!not && readIf(IS)) {
            r = readConditionIs(r, whenOperand);
        } else {
            switch (currentTokenType) {
            case BETWEEN: {
                read();
                boolean symmetric = readIf(SYMMETRIC);
                if (!symmetric) {
                    readIf(ASYMMETRIC);
                }
                Expression a = readConcat();
                read(AND);
                r = new BetweenPredicate(r, not, whenOperand, symmetric, a, readConcat());
                break;
            }
            case IN:
                read();
                r = readInPredicate(r, not, whenOperand);
                break;
            case LIKE: {
                read();
                r = readLikePredicate(r, LikeType.LIKE, not, whenOperand);
                break;
            }
            default:
                if (readIf("ILIKE")) {
                    r = readLikePredicate(r, LikeType.ILIKE, not, whenOperand);
                } else if (readIf("REGEXP")) {
                    Expression b = readConcat();
                    recompileAlways = true;
                    r = new CompareLike(database, r, not, whenOperand, b, null, LikeType.REGEXP);
                } else if (not) {
                    if (whenOperand) {
                        return null;
                    }
                    if (expectedList != null) {
                        addMultipleExpected(BETWEEN, IN, LIKE);
                    }
                    throw getSyntaxError();
                } else {
                    int compareType = getCompareType(currentTokenType);
                    if (compareType < 0) {
                        return null;
                    }
                    read();
                    r = readComparison(r, compareType, whenOperand);
                }
            }
        }
        return r;
    }

    private Expression readConditionIs(Expression left, boolean whenOperand) {
        boolean isNot = readIf(NOT);
        switch (currentTokenType) {
        case NULL:
            read();
            left = new NullPredicate(left, isNot, whenOperand);
            break;
        case DISTINCT:
            read();
            read(FROM);
            left = readComparison(left, isNot ? Comparison.EQUAL_NULL_SAFE : Comparison.NOT_EQUAL_NULL_SAFE,
                    whenOperand);
            break;
        case TRUE:
            read();
            left = new BooleanTest(left, isNot, whenOperand, true);
            break;
        case FALSE:
            read();
            left = new BooleanTest(left, isNot, whenOperand, false);
            break;
        case UNKNOWN:
            read();
            left = new BooleanTest(left, isNot, whenOperand, null);
            break;
        default:
            if (readIf("OF")) {
                left = readTypePredicate(left, isNot, whenOperand);
            } else if (readIf("JSON")) {
                left = readJsonPredicate(left, isNot, whenOperand);
            } else {
                if (expectedList != null) {
                    addMultipleExpected(NULL, DISTINCT, TRUE, FALSE, UNKNOWN);
                }
                /*
                 * Databases that were created in 1.4.199 and older
                 * versions can contain invalid generated IS [ NOT ]
                 * expressions.
                 */
                if (whenOperand || !session.isQuirksMode()) {
                    throw getSyntaxError();
                }
                left = new Comparison(isNot ? Comparison.NOT_EQUAL_NULL_SAFE : Comparison.EQUAL_NULL_SAFE, left,
                        readConcat(), false);
            }
        }
        return left;
    }

    private TypePredicate readTypePredicate(Expression left, boolean not, boolean whenOperand) {
        read(OPEN_PAREN);
        ArrayList<TypeInfo> typeList = Utils.newSmallArrayList();
        do {
            typeList.add(parseDataType());
        } while (readIfMore());
        return new TypePredicate(left, not, whenOperand, typeList.toArray(new TypeInfo[0]));
    }

    private Expression readInPredicate(Expression left, boolean not, boolean whenOperand) {
        read(OPEN_PAREN);
        if (!whenOperand && database.getMode().allowEmptyInPredicate && readIf(CLOSE_PAREN)) {
            return ValueExpression.getBoolean(not);
        }
        ArrayList<Expression> v;
        if (isQuery()) {
            Query query = parseQuery();
            if (!readIfMore()) {
                return new ConditionInQuery(left, not, whenOperand, query, false, Comparison.EQUAL);
            }
            v = Utils.newSmallArrayList();
            v.add(new Subquery(query));
        } else {
            v = Utils.newSmallArrayList();
        }
        do {
            v.add(readExpression());
        } while (readIfMore());
        return new ConditionIn(left, not, whenOperand, v);
    }

    private IsJsonPredicate readJsonPredicate(Expression left, boolean not, boolean whenOperand) {
        JSONItemType itemType;
        if (readIf(VALUE)) {
            itemType = JSONItemType.VALUE;
        } else if (readIf(ARRAY)) {
            itemType = JSONItemType.ARRAY;
        } else if (readIf("OBJECT")) {
            itemType = JSONItemType.OBJECT;
        } else if (readIf("SCALAR")) {
            itemType = JSONItemType.SCALAR;
        } else {
            itemType = JSONItemType.VALUE;
        }
        boolean unique = false;
        if (readIf(WITH)) {
            read(UNIQUE);
            readIf("KEYS");
            unique = true;
        } else if (readIf("WITHOUT")) {
            read(UNIQUE);
            readIf("KEYS");
        }
        return new IsJsonPredicate(left, not, whenOperand, unique, itemType);
    }

    private Expression readLikePredicate(Expression left, LikeType likeType, boolean not, boolean whenOperand) {
        Expression right = readConcat();
        Expression esc = readIf("ESCAPE") ? readConcat() : null;
        recompileAlways = true;
        return new CompareLike(database, left, not, whenOperand, right, esc, likeType);
    }

    private Expression readComparison(Expression left, int compareType, boolean whenOperand) {
        int start = lastParseIndex;
        if (readIf(ALL)) {
            read(OPEN_PAREN);
            if (isQuery()) {
                Query query = parseQuery();
                left = new ConditionInQuery(left, false, whenOperand, query, true, compareType);
                read(CLOSE_PAREN);
            } else {
                reread(start);
                left = new Comparison(compareType, left, readConcat(), whenOperand);
            }
        } else if (readIf(ANY) || readIf(SOME)) {
            read(OPEN_PAREN);
            if (currentTokenType == PARAMETER && compareType == Comparison.EQUAL) {
                Parameter p = readParameter();
                left = new ConditionInParameter(left, false, whenOperand, p);
                read(CLOSE_PAREN);
            } else if (isQuery()) {
                Query query = parseQuery();
                left = new ConditionInQuery(left, false, whenOperand, query, false, compareType);
                read(CLOSE_PAREN);
            } else {
                reread(start);
                left = new Comparison(compareType, left, readConcat(), whenOperand);
            }
        } else {
            left = new Comparison(compareType, left, readConcat(), whenOperand);
        }
        return left;
    }

    private Expression readConcat() {
        Expression op1 = readSum();
        for (;;) {
            switch (currentTokenType) {
            case CONCATENATION: {
                read();
                Expression op2 = readSum();
                if (readIf(CONCATENATION)) {
                    ConcatenationOperation c = new ConcatenationOperation();
                    c.addParameter(op1);
                    c.addParameter(op2);
                    do {
                        c.addParameter(readSum());
                    } while (readIf(CONCATENATION));
                    c.doneWithParameters();
                    op1 = c;
                } else {
                    op1 = new ConcatenationOperation(op1, op2);
                }
                break;
            }
            case TILDE: // PostgreSQL compatibility
                op1 = readTildeCondition(op1, false);
                break;
            case NOT_TILDE: // PostgreSQL compatibility
                op1 = readTildeCondition(op1, true);
                break;
            default:
                // Don't add compatibility operators
                addExpected(CONCATENATION);
                return op1;
            }
        }
    }

    private Expression readSum() {
        Expression r = readFactor();
        while (true) {
            if (readIf(PLUS_SIGN)) {
                r = new BinaryOperation(OpType.PLUS, r, readFactor());
            } else if (readIf(MINUS_SIGN)) {
                r = new BinaryOperation(OpType.MINUS, r, readFactor());
            } else {
                return r;
            }
        }
    }

    private Expression readFactor() {
        Expression r = readTerm();
        while (true) {
            if (readIf(ASTERISK)) {
                r = new BinaryOperation(OpType.MULTIPLY, r, readTerm());
            } else if (readIf(SLASH)) {
                r = new BinaryOperation(OpType.DIVIDE, r, readTerm());
            } else if (readIf(PERCENT)) {
                r = new MathFunction(r, readTerm(), MathFunction.MOD);
            } else {
                return r;
            }
        }
    }

    private Expression readTildeCondition(Expression r, boolean not) {
        read();
        if (readIf(ASTERISK)) {
            r = new CastSpecification(r, TypeInfo.TYPE_VARCHAR_IGNORECASE);
        }
        return new CompareLike(database, r, not, false, readSum(), null, LikeType.REGEXP);
    }

    private Expression readAggregate(AggregateType aggregateType, String aggregateName) {
        if (currentSelect == null) {
            throw getSyntaxError();
        }
        Aggregate r;
        switch (aggregateType) {
        case COUNT:
            if (readIf(ASTERISK)) {
                r = new Aggregate(AggregateType.COUNT_ALL, new Expression[0], currentSelect, false);
            } else {
                boolean distinct = readDistinctAgg();
                Expression on = readExpression();
                if (on instanceof Wildcard && !distinct) {
                    // PostgreSQL compatibility: count(t.*)
                    r = new Aggregate(AggregateType.COUNT_ALL, new Expression[0], currentSelect, false);
                } else {
                    r = new Aggregate(AggregateType.COUNT, new Expression[] { on }, currentSelect, distinct);
                }
            }
            break;
        case COVAR_POP:
        case COVAR_SAMP:
        case CORR:
        case REGR_SLOPE:
        case REGR_INTERCEPT:
        case REGR_COUNT:
        case REGR_R2:
        case REGR_AVGX:
        case REGR_AVGY:
        case REGR_SXX:
        case REGR_SYY:
        case REGR_SXY:
            r = new Aggregate(aggregateType, new Expression[] { readExpression(), readNextArgument() },
                    currentSelect, false);
            break;
        case HISTOGRAM:
            r = new Aggregate(aggregateType, new Expression[] { readExpression() }, currentSelect, false);
            break;
        case LISTAGG: {
            boolean distinct = readDistinctAgg();
            Expression arg = readExpression();
            ListaggArguments extraArguments = new ListaggArguments();
            ArrayList<QueryOrderBy> orderByList;
            if ("STRING_AGG".equals(aggregateName)) {
                // PostgreSQL compatibility: string_agg(expression, delimiter)
                read(COMMA);
                extraArguments.setSeparator(readString());
                orderByList = readIfOrderBy();
            } else if ("GROUP_CONCAT".equals(aggregateName)) {
                orderByList = readIfOrderBy();
                if (readIf("SEPARATOR")) {
                    extraArguments.setSeparator(readString());
                }
            } else {
                if (readIf(COMMA)) {
                    extraArguments.setSeparator(readString());
                }
                if (readIf(ON)) {
                    read("OVERFLOW");
                    if (readIf("TRUNCATE")) {
                        extraArguments.setOnOverflowTruncate(true);
                        if (currentTokenType == LITERAL) {
                            extraArguments.setFilter(readString());
                        }
                        if (!readIf(WITH)) {
                            read("WITHOUT");
                            extraArguments.setWithoutCount(true);
                        }
                        read("COUNT");
                    } else {
                        read("ERROR");
                    }
                }
                orderByList = null;
            }
            Expression[] args = new Expression[] { arg };
            int index = lastParseIndex;
            read(CLOSE_PAREN);
            if (orderByList == null && isToken("WITHIN")) {
                r = readWithinGroup(aggregateType, args, distinct, extraArguments, false, false);
            } else {
                reread(index);
                r = new Aggregate(AggregateType.LISTAGG, args, currentSelect, distinct);
                r.setExtraArguments(extraArguments);
                if (orderByList != null) {
                    r.setOrderByList(orderByList);
                }
            }
            break;
        }
        case ARRAY_AGG: {
            boolean distinct = readDistinctAgg();
            r = new Aggregate(AggregateType.ARRAY_AGG, new Expression[] { readExpression() }, currentSelect, distinct);
            r.setOrderByList(readIfOrderBy());
            break;
        }
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST: {
            if (isToken(CLOSE_PAREN)) {
                return readWindowFunction(aggregateName);
            }
            ArrayList<Expression> expressions = Utils.newSmallArrayList();
            do {
                expressions.add(readExpression());
            } while (readIfMore());
            r = readWithinGroup(aggregateType, expressions.toArray(new Expression[0]), false, null, true, false);
            break;
        }
        case PERCENTILE_CONT:
        case PERCENTILE_DISC: {
            Expression num = readExpression();
            read(CLOSE_PAREN);
            r = readWithinGroup(aggregateType, new Expression[] { num }, false, null, false, true);
            break;
        }
        case MODE: {
            if (readIf(CLOSE_PAREN)) {
                r = readWithinGroup(AggregateType.MODE, new Expression[0], false, null, false, true);
            } else {
                Expression expr = readExpression();
                r = new Aggregate(AggregateType.MODE, new Expression[0], currentSelect, false);
                if (readIf(ORDER)) {
                    read("BY");
                    Expression expr2 = readExpression();
                    String sql = expr.getSQL(HasSQL.DEFAULT_SQL_FLAGS), sql2 = expr2.getSQL(HasSQL.DEFAULT_SQL_FLAGS);
                    if (!sql.equals(sql2)) {
                        throw DbException.getSyntaxError(ErrorCode.IDENTICAL_EXPRESSIONS_SHOULD_BE_USED, sqlCommand,
                                lastParseIndex, sql, sql2);
                    }
                    readAggregateOrder(r, expr, true);
                } else {
                    readAggregateOrder(r, expr, false);
                }
            }
            break;
        }
        case JSON_OBJECTAGG: {
            boolean withKey = readIf(KEY);
            Expression key = readExpression();
            if (withKey) {
                read(VALUE);
            } else if (!readIf(VALUE)) {
                read(COLON);
            }
            Expression value = readExpression();
            r = new Aggregate(AggregateType.JSON_OBJECTAGG, new Expression[] { key, value }, currentSelect, false);
            readJsonObjectFunctionFlags(r, false);
            break;
        }
        case JSON_ARRAYAGG: {
            boolean distinct = readDistinctAgg();
            r = new Aggregate(AggregateType.JSON_ARRAYAGG, new Expression[] { readExpression() }, currentSelect,
                    distinct);
            r.setOrderByList(readIfOrderBy());
            r.setFlags(JsonConstructorUtils.JSON_ABSENT_ON_NULL);
            readJsonObjectFunctionFlags(r, true);
            break;
        }
        default:
            boolean distinct = readDistinctAgg();
            r = new Aggregate(aggregateType, new Expression[] { readExpression() }, currentSelect, distinct);
            break;
        }
        read(CLOSE_PAREN);
        readFilterAndOver(r);
        return r;
    }

    private Aggregate readWithinGroup(AggregateType aggregateType, Expression[] args, boolean distinct,
            Object extraArguments, boolean forHypotheticalSet, boolean simple) {
        read("WITHIN");
        read(GROUP);
        read(OPEN_PAREN);
        read(ORDER);
        read("BY");
        Aggregate r = new Aggregate(aggregateType, args, currentSelect, distinct);
        r.setExtraArguments(extraArguments);
        if (forHypotheticalSet) {
            int count = args.length;
            ArrayList<QueryOrderBy> orderList = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    read(COMMA);
                }
                orderList.add(parseSortSpecification());
            }
            r.setOrderByList(orderList);
        } else if (simple) {
            readAggregateOrder(r, readExpression(), true);
        } else {
            r.setOrderByList(parseSortSpecificationList());
        }
        return r;
    }

    private void readAggregateOrder(Aggregate r, Expression expr, boolean parseSortType) {
        ArrayList<QueryOrderBy> orderList = new ArrayList<>(1);
        QueryOrderBy order = new QueryOrderBy();
        order.expression = expr;
        if (parseSortType) {
            order.sortType = parseSortType();
        }
        orderList.add(order);
        r.setOrderByList(orderList);
    }

    private ArrayList<QueryOrderBy> readIfOrderBy() {
        if (readIf(ORDER)) {
            read("BY");
            return parseSortSpecificationList();
        }
        return null;
    }

    private ArrayList<QueryOrderBy> parseSortSpecificationList() {
        ArrayList<QueryOrderBy> orderList = Utils.newSmallArrayList();
        do {
            orderList.add(parseSortSpecification());
        } while (readIf(COMMA));
        return orderList;
    }

    private QueryOrderBy parseSortSpecification() {
        QueryOrderBy order = new QueryOrderBy();
        order.expression = readExpression();
        order.sortType = parseSortType();
        return order;
    }

    private Expression readUserDefinedFunctionIf(Schema schema, String functionName) {
        UserDefinedFunction userDefinedFunction = findUserDefinedFunctionWithinPath(schema, functionName);
        if (userDefinedFunction == null) {
            return null;
        } else if (userDefinedFunction instanceof FunctionAlias) {
            FunctionAlias functionAlias = (FunctionAlias) userDefinedFunction;
            ArrayList<Expression> argList = Utils.newSmallArrayList();
            if (!readIf(CLOSE_PAREN)) {
                do {
                    argList.add(readExpression());
                } while (readIfMore());
            }
            return new JavaFunction(functionAlias, argList.toArray(new Expression[0]));
        } else {
            UserAggregate aggregate = (UserAggregate) userDefinedFunction;
            boolean distinct = readDistinctAgg();
            ArrayList<Expression> params = Utils.newSmallArrayList();
            do {
                params.add(readExpression());
            } while (readIfMore());
            Expression[] list = params.toArray(new Expression[0]);
            JavaAggregate agg = new JavaAggregate(aggregate, list, currentSelect, distinct);
            readFilterAndOver(agg);
            return agg;
        }
    }

    private boolean readDistinctAgg() {
        if (readIf(DISTINCT)) {
            return true;
        }
        readIf(ALL);
        return false;
    }

    private void readFilterAndOver(AbstractAggregate aggregate) {
        if (readIf("FILTER")) {
            read(OPEN_PAREN);
            read(WHERE);
            Expression filterCondition = readExpression();
            read(CLOSE_PAREN);
            aggregate.setFilterCondition(filterCondition);
        }
        readOver(aggregate);
    }

    private void readOver(DataAnalysisOperation operation) {
        if (readIf("OVER")) {
            operation.setOverCondition(readWindowNameOrSpecification());
            currentSelect.setWindowQuery();
        } else if (operation.isAggregate()) {
            currentSelect.setGroupQuery();
        } else {
            throw getSyntaxError();
        }
    }

    private Window readWindowNameOrSpecification() {
        return isToken(OPEN_PAREN) ? readWindowSpecification() : new Window(readIdentifier(), null, null, null);
    }

    private Window readWindowSpecification() {
        read(OPEN_PAREN);
        String parent = null;
        if (currentTokenType == IDENTIFIER) {
            String token = currentToken;
            if (currentTokenQuoted || ( //
                    !equalsToken(token, "PARTITION") //
                    && !equalsToken(token, "ROWS") //
                    && !equalsToken(token, "RANGE") //
                    && !equalsToken(token, "GROUPS"))) {
                parent = token;
                read();
            }
        }
        ArrayList<Expression> partitionBy = null;
        if (readIf("PARTITION")) {
            read("BY");
            partitionBy = Utils.newSmallArrayList();
            do {
                Expression expr = readExpression();
                partitionBy.add(expr);
            } while (readIf(COMMA));
        }
        ArrayList<QueryOrderBy> orderBy = readIfOrderBy();
        WindowFrame frame = readWindowFrame();
        read(CLOSE_PAREN);
        return new Window(parent, partitionBy, orderBy, frame);
    }

    private WindowFrame readWindowFrame() {
        WindowFrameUnits units;
        if (readIf("ROWS")) {
            units = WindowFrameUnits.ROWS;
        } else if (readIf("RANGE")) {
            units = WindowFrameUnits.RANGE;
        } else if (readIf("GROUPS")) {
            units = WindowFrameUnits.GROUPS;
        } else {
            return null;
        }
        WindowFrameBound starting, following;
        if (readIf(BETWEEN)) {
            starting = readWindowFrameRange();
            read(AND);
            following = readWindowFrameRange();
        } else {
            starting = readWindowFrameStarting();
            following = null;
        }
        int idx = lastParseIndex;
        WindowFrameExclusion exclusion = WindowFrameExclusion.EXCLUDE_NO_OTHERS;
        if (readIf("EXCLUDE")) {
            if (readIf("CURRENT")) {
                read(ROW);
                exclusion = WindowFrameExclusion.EXCLUDE_CURRENT_ROW;
            } else if (readIf(GROUP)) {
                exclusion = WindowFrameExclusion.EXCLUDE_GROUP;
            } else if (readIf("TIES")) {
                exclusion = WindowFrameExclusion.EXCLUDE_TIES;
            } else {
                read("NO");
                read("OTHERS");
            }
        }
        WindowFrame frame = new WindowFrame(units, starting, following, exclusion);
        if (!frame.isValid()) {
            throw DbException.getSyntaxError(sqlCommand, idx);
        }
        return frame;
    }

    private WindowFrameBound readWindowFrameStarting() {
        if (readIf("UNBOUNDED")) {
            read("PRECEDING");
            return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_PRECEDING, null);
        }
        if (readIf("CURRENT")) {
            read(ROW);
            return new WindowFrameBound(WindowFrameBoundType.CURRENT_ROW, null);
        }
        Expression value = readExpression();
        read("PRECEDING");
        return new WindowFrameBound(WindowFrameBoundType.PRECEDING, value);
    }

    private WindowFrameBound readWindowFrameRange() {
        if (readIf("UNBOUNDED")) {
            if (readIf("PRECEDING")) {
                return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_PRECEDING, null);
            }
            read("FOLLOWING");
            return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_FOLLOWING, null);
        }
        if (readIf("CURRENT")) {
            read(ROW);
            return new WindowFrameBound(WindowFrameBoundType.CURRENT_ROW, null);
        }
        Expression value = readExpression();
        if (readIf("PRECEDING")) {
            return new WindowFrameBound(WindowFrameBoundType.PRECEDING, value);
        }
        read("FOLLOWING");
        return new WindowFrameBound(WindowFrameBoundType.FOLLOWING, value);
    }

    private Expression readFunction(Schema schema, String name) {
        String upperName = upperName(name);
        if (schema != null) {
            return readFunctionWithSchema(schema, name, upperName);
        }
        boolean allowOverride = database.isAllowBuiltinAliasOverride();
        if (allowOverride) {
            Expression e = readUserDefinedFunctionIf(null, name);
            if (e != null) {
                return e;
            }
        }
        AggregateType agg = Aggregate.getAggregateType(upperName);
        if (agg != null) {
            return readAggregate(agg, upperName);
        }
        Expression e = readBuiltinFunctionIf(upperName);
        if (e != null) {
            return e;
        }
        e = readWindowFunction(upperName);
        if (e != null) {
            return e;
        }
        e = readCompatibilityFunction(upperName);
        if (e != null) {
            return e;
        }
        if (!allowOverride) {
            e = readUserDefinedFunctionIf(null, name);
            if (e != null) {
                return e;
            }
        }
        throw DbException.get(ErrorCode.FUNCTION_NOT_FOUND_1, name);
    }

    private Expression readFunctionWithSchema(Schema schema, String name, String upperName) {
        if (database.getMode().getEnum() == ModeEnum.PostgreSQL
                && schema.getName().equals(database.sysIdentifier("PG_CATALOG"))) {
            FunctionsPostgreSQL function = FunctionsPostgreSQL.getFunction(upperName);
            if (function != null) {
                return readParameters(function);
            }
        }
        Expression function = readUserDefinedFunctionIf(schema, name);
        if (function != null) {
            return function;
        }
        throw DbException.get(ErrorCode.FUNCTION_NOT_FOUND_1, name);
    }

    private Expression readCompatibilityFunction(String name) {
        switch (name) {
        // ||
        case "ARRAY_APPEND":
        case "ARRAY_CAT":
            return new ConcatenationOperation(readExpression(), readLastArgument());
        // []
        case "ARRAY_GET":
            return new ArrayElementReference(readExpression(), readLastArgument());
        // CARDINALITY
        case "ARRAY_LENGTH":
            return new CardinalityExpression(readSingleArgument(), false);
        // Simple case
        case "DECODE": {
            Expression caseOperand = readExpression();
            boolean canOptimize = caseOperand.isConstant() && !caseOperand.getValue(session).containsNull();
            Expression a = readNextArgument(), b = readNextArgument();
            SimpleCase.SimpleWhen when = decodeToWhen(caseOperand, canOptimize, a, b), current = when;
            Expression elseResult = null;
            while (readIf(COMMA)) {
                a = readExpression();
                if (readIf(COMMA)) {
                    b = readExpression();
                    SimpleCase.SimpleWhen next = decodeToWhen(caseOperand, canOptimize, a, b);
                    current.setWhen(next);
                    current = next;
                } else {
                    elseResult = a;
                    break;
                }
            }
            read(CLOSE_PAREN);
            return new SimpleCase(caseOperand, when, elseResult);
        }
        // Searched case
        case "CASEWHEN":
            return readCompatibilityCase(readExpression());
        case "NVL2":
            return readCompatibilityCase(new NullPredicate(readExpression(), true, false));
        // Cast specification
        case "CONVERT": {
            Expression arg;
            Column column;
            if (database.getMode().swapConvertFunctionParameters) {
                column = parseColumnWithType(null);
                arg = readNextArgument();
            } else {
                arg = readExpression();
                read(COMMA);
                column = parseColumnWithType(null);
            }
            read(CLOSE_PAREN);
            return new CastSpecification(arg, column);
        }
        // COALESCE
        case "IFNULL":
            return new CoalesceFunction(CoalesceFunction.COALESCE, readExpression(), readLastArgument());
        case "NVL":
            return readCoalesceFunction(CoalesceFunction.COALESCE);
        // CURRENT_CATALOG
        case "DATABASE":
            read(CLOSE_PAREN);
            return new CurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_CATALOG);
        // CURRENT_DATE
        case "CURDATE":
        case "SYSDATE":
        case "TODAY":
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_DATE, true, name);
        // CURRENT_SCHEMA
        case "SCHEMA":
            read(CLOSE_PAREN);
            return new CurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_SCHEMA);
        // CURRENT_TIMESTAMP
        case "SYSTIMESTAMP":
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_TIMESTAMP, true, name);
        // EXTRACT
        case "DAY":
        case "DAY_OF_MONTH":
        case "DAYOFMONTH":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.DAY, readSingleArgument(), null);
        case "DAY_OF_WEEK":
        case "DAYOFWEEK":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.DAY_OF_WEEK, readSingleArgument(),
                    null);
        case "DAY_OF_YEAR":
        case "DAYOFYEAR":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.DAY_OF_YEAR, readSingleArgument(),
                    null);
        case "HOUR":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.HOUR, readSingleArgument(), null);
        case "ISO_DAY_OF_WEEK":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.ISO_DAY_OF_WEEK,
                    readSingleArgument(), null);
        case "ISO_WEEK":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.ISO_WEEK, readSingleArgument(),
                    null);
        case "ISO_YEAR":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.ISO_WEEK_YEAR, readSingleArgument(),
                    null);
        case "MINUTE":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.MINUTE, readSingleArgument(), null);
        case "MONTH":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.MONTH, readSingleArgument(), null);
        case "QUARTER":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.QUARTER, readSingleArgument(), //
                    null);
        case "SECOND":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.SECOND, readSingleArgument(), null);
        case "WEEK":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.WEEK, readSingleArgument(), null);
        case "YEAR":
            return new DateTimeFunction(DateTimeFunction.EXTRACT, DateTimeFunction.YEAR, readSingleArgument(), null);
        // LOCALTIME
        case "CURTIME":
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIME, true, "CURTIME");
        case "SYSTIME":
            read(CLOSE_PAREN);
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIME, false, "SYSTIME");
        // LOCALTIMESTAMP
        case "NOW":
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, true, "NOW");
        // LOCATE
        case "INSTR": {
            Expression arg1 = readExpression();
            return new StringFunction(readNextArgument(), arg1, readIfArgument(), StringFunction.LOCATE);
        }
        case "POSITION": {
            // can't read expression because IN would be read too early
            Expression arg1 = readConcat();
            if (!readIf(COMMA)) {
                read(IN);
            }
            return new StringFunction(arg1, readSingleArgument(), null, StringFunction.LOCATE);
        }
        // LOWER
        case "LCASE":
            return new StringFunction1(readSingleArgument(), StringFunction1.LOWER);
        // SUBSTRING
        case "SUBSTR":
            return readSubstringFunction();
        // TRIM
        case "LTRIM":
            return new TrimFunction(readSingleArgument(), null, TrimFunction.LEADING);
        case "RTRIM":
            return new TrimFunction(readSingleArgument(), null, TrimFunction.TRAILING);
        // UPPER
        case "UCASE":
            return new StringFunction1(readSingleArgument(), StringFunction1.UPPER);
        // Sequence value
        case "CURRVAL":
            return readCompatibilitySequenceValueFunction(true);
        case "NEXTVAL":
            return readCompatibilitySequenceValueFunction(false);
        default:
            return null;
        }
    }

    private <T extends ExpressionWithVariableParameters> T readParameters(T expression) {
        if (!readIf(CLOSE_PAREN)) {
            do {
                expression.addParameter(readExpression());
            } while (readIfMore());
        }
        expression.doneWithParameters();
        return expression;
    }

    private SimpleCase.SimpleWhen decodeToWhen(Expression caseOperand, boolean canOptimize, Expression whenOperand,
            Expression result) {
        if (!canOptimize && (!whenOperand.isConstant() || whenOperand.getValue(session).containsNull())) {
            whenOperand = new Comparison(Comparison.EQUAL_NULL_SAFE, caseOperand, whenOperand, true);
        }
        return new SimpleCase.SimpleWhen(whenOperand, result);
    }

    private Expression readCompatibilityCase(Expression when) {
        return new SearchedCase(new Expression[] { when, readNextArgument(), readLastArgument() });
    }

    private Expression readCompatibilitySequenceValueFunction(boolean current) {
        Expression arg1 = readExpression(), arg2 = readIf(COMMA) ? readExpression() : null;
        read(CLOSE_PAREN);
        return new CompatibilitySequenceValueFunction(arg1, arg2, current);
    }

    private Expression readBuiltinFunctionIf(String upperName) {
        switch (upperName) {
        case "ABS":
            return new MathFunction(readSingleArgument(), null, MathFunction.ABS);
        case "MOD":
            return new MathFunction(readExpression(), readLastArgument(), MathFunction.MOD);
        case "SIN":
            return new MathFunction1(readSingleArgument(), MathFunction1.SIN);
        case "COS":
            return new MathFunction1(readSingleArgument(), MathFunction1.COS);
        case "TAN":
            return new MathFunction1(readSingleArgument(), MathFunction1.TAN);
        case "COT":
            return new MathFunction1(readSingleArgument(), MathFunction1.COT);
        case "SINH":
            return new MathFunction1(readSingleArgument(), MathFunction1.SINH);
        case "COSH":
            return new MathFunction1(readSingleArgument(), MathFunction1.COSH);
        case "TANH":
            return new MathFunction1(readSingleArgument(), MathFunction1.TANH);
        case "ASIN":
            return new MathFunction1(readSingleArgument(), MathFunction1.ASIN);
        case "ACOS":
            return new MathFunction1(readSingleArgument(), MathFunction1.ACOS);
        case "ATAN":
            return new MathFunction1(readSingleArgument(), MathFunction1.ATAN);
        case "ATAN2":
            return new MathFunction2(readExpression(), readLastArgument(), MathFunction2.ATAN2);
        case "LOG": {
            Expression arg1 = readExpression();
            if (readIf(COMMA)) {
                return new MathFunction2(arg1, readSingleArgument(), MathFunction2.LOG);
            } else {
                read(CLOSE_PAREN);
                return new MathFunction1(arg1,
                        database.getMode().logIsLogBase10 ? MathFunction1.LOG10 : MathFunction1.LN);
            }
        }
        case "LOG10":
            return new MathFunction1(readSingleArgument(), MathFunction1.LOG10);
        case "LN":
            return new MathFunction1(readSingleArgument(), MathFunction1.LN);
        case "EXP":
            return new MathFunction1(readSingleArgument(), MathFunction1.EXP);
        case "POWER":
            return new MathFunction2(readExpression(), readLastArgument(), MathFunction2.POWER);
        case "SQRT":
            return new MathFunction1(readSingleArgument(), MathFunction1.SQRT);
        case "FLOOR":
            return new MathFunction(readSingleArgument(), null, MathFunction.FLOOR);
        case "CEIL":
        case "CEILING":
            return new MathFunction(readSingleArgument(), null, MathFunction.CEIL);
        case "ROUND":
            return new MathFunction(readExpression(), readIfArgument(), MathFunction.ROUND);
        case "ROUNDMAGIC":
            return new MathFunction(readSingleArgument(), null, MathFunction.ROUNDMAGIC);
        case "SIGN":
            return new MathFunction(readSingleArgument(), null, MathFunction.SIGN);
        case "TRUNC":
        case "TRUNCATE":
            return new MathFunction(readExpression(), readIfArgument(), MathFunction.TRUNC);
        case "DEGREES":
            return new MathFunction1(readSingleArgument(), MathFunction1.DEGREES);
        case "RADIANS":
            return new MathFunction1(readSingleArgument(), MathFunction1.RADIANS);
        case "BITAND":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITAND);
        case "BITOR":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITOR);
        case "BITXOR":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITXOR);
        case "BITNOT":
            return new BitFunction(readSingleArgument(), null, BitFunction.BITNOT);
        case "BITNAND":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITNAND);
        case "BITNOR":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITNOR);
        case "BITXNOR":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITXNOR);
        case "BITGET":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.BITGET);
        case "BITCOUNT":
            return new BitFunction(readSingleArgument(), null, BitFunction.BITCOUNT);
        case "LSHIFT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.LSHIFT);
        case "RSHIFT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.RSHIFT);
        case "ULSHIFT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.ULSHIFT);
        case "URSHIFT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.URSHIFT);
        case "ROTATELEFT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.ROTATELEFT);
        case "ROTATERIGHT":
            return new BitFunction(readExpression(), readLastArgument(), BitFunction.ROTATERIGHT);
        case "EXTRACT": {
            int field = readDateTimeField();
            read(FROM);
            return new DateTimeFunction(DateTimeFunction.EXTRACT, field, readSingleArgument(), null);
        }
        case "DATE_TRUNC":
            return new DateTimeFunction(DateTimeFunction.DATE_TRUNC, readDateTimeField(), readLastArgument(), null);
        case "DATEADD":
        case "TIMESTAMPADD":
            return new DateTimeFunction(DateTimeFunction.DATEADD, readDateTimeField(), readNextArgument(),
                    readLastArgument());
        case "DATEDIFF":
        case "TIMESTAMPDIFF":
            return new DateTimeFunction(DateTimeFunction.DATEDIFF, readDateTimeField(), readNextArgument(),
                    readLastArgument());
        case "FORMATDATETIME":
            return readDateTimeFormatFunction(DateTimeFormatFunction.FORMATDATETIME);
        case "PARSEDATETIME":
            return readDateTimeFormatFunction(DateTimeFormatFunction.PARSEDATETIME);
        case "DAYNAME":
            return new DayMonthNameFunction(readSingleArgument(), DayMonthNameFunction.DAYNAME);
        case "MONTHNAME":
            return new DayMonthNameFunction(readSingleArgument(), DayMonthNameFunction.MONTHNAME);
        case "CARDINALITY":
            return new CardinalityExpression(readSingleArgument(), false);
        case "ARRAY_MAX_CARDINALITY":
            return new CardinalityExpression(readSingleArgument(), true);
        case "LOCATE":
            return new StringFunction(readExpression(), readNextArgument(), readIfArgument(), StringFunction.LOCATE);
        case "INSERT":
            return new StringFunction(readExpression(), readNextArgument(), readNextArgument(), readLastArgument(),
                    StringFunction.INSERT);
        case "REPLACE":
            return new StringFunction(readExpression(), readNextArgument(), readIfArgument(), StringFunction.REPLACE);
        case "LPAD":
            return new StringFunction(readExpression(), readNextArgument(), readIfArgument(), StringFunction.LPAD);
        case "RPAD":
            return new StringFunction(readExpression(), readNextArgument(), readIfArgument(), StringFunction.RPAD);
        case "TRANSLATE":
            return new StringFunction(readExpression(), readNextArgument(), readLastArgument(),
                    StringFunction.TRANSLATE);
        case "UPPER":
            return new StringFunction1(readSingleArgument(), StringFunction1.UPPER);
        case "LOWER":
            return new StringFunction1(readSingleArgument(), StringFunction1.LOWER);
        case "ASCII":
            return new StringFunction1(readSingleArgument(), StringFunction1.ASCII);
        case "CHAR":
        case "CHR":
            return new StringFunction1(readSingleArgument(), StringFunction1.CHAR);
        case "STRINGENCODE":
            return new StringFunction1(readSingleArgument(), StringFunction1.STRINGENCODE);
        case "STRINGDECODE":
            return new StringFunction1(readSingleArgument(), StringFunction1.STRINGDECODE);
        case "STRINGTOUTF8":
            return new StringFunction1(readSingleArgument(), StringFunction1.STRINGTOUTF8);
        case "UTF8TOSTRING":
            return new StringFunction1(readSingleArgument(), StringFunction1.UTF8TOSTRING);
        case "HEXTORAW":
            return new StringFunction1(readSingleArgument(), StringFunction1.HEXTORAW);
        case "RAWTOHEX":
            return new StringFunction1(readSingleArgument(), StringFunction1.RAWTOHEX);
        case "SPACE":
            return new StringFunction1(readSingleArgument(), StringFunction1.SPACE);
        case "QUOTE_IDENT":
            return new StringFunction1(readSingleArgument(), StringFunction1.QUOTE_IDENT);
        case "SUBSTRING":
            return readSubstringFunction();
        case "TO_CHAR": {
            Expression arg1 = readExpression(), arg2, arg3;
            if (readIf(COMMA)) {
                arg2 = readExpression();
                arg3 = readIf(COMMA) ? readExpression() : null;
            } else {
                arg3 = arg2 = null;
            }
            read(CLOSE_PAREN);
            return new ToCharFunction(arg1, arg2, arg3);
        }
        case "REPEAT":
            return new StringFunction2(readExpression(), readLastArgument(), StringFunction2.REPEAT);
        case "CHAR_LENGTH":
        case "CHARACTER_LENGTH":
        case "LENGTH":
            return new LengthFunction(readIfSingleArgument(), LengthFunction.CHAR_LENGTH);
        case "OCTET_LENGTH":
            return new LengthFunction(readIfSingleArgument(), LengthFunction.OCTET_LENGTH);
        case "BIT_LENGTH":
            return new LengthFunction(readIfSingleArgument(), LengthFunction.BIT_LENGTH);
        case "TRIM":
            return readTrimFunction();
        case "REGEXP_LIKE":
            return readParameters(new RegexpFunction(RegexpFunction.REGEXP_LIKE));
        case "REGEXP_REPLACE":
            return readParameters(new RegexpFunction(RegexpFunction.REGEXP_REPLACE));
        case "REGEXP_SUBSTR":
            return readParameters(new RegexpFunction(RegexpFunction.REGEXP_SUBSTR));
        case "XMLATTR":
            return readParameters(new XMLFunction(XMLFunction.XMLATTR));
        case "XMLCDATA":
            return readParameters(new XMLFunction(XMLFunction.XMLCDATA));
        case "XMLCOMMENT":
            return readParameters(new XMLFunction(XMLFunction.XMLCOMMENT));
        case "XMLNODE":
            return readParameters(new XMLFunction(XMLFunction.XMLNODE));
        case "XMLSTARTDOC":
            return readParameters(new XMLFunction(XMLFunction.XMLSTARTDOC));
        case "XMLTEXT":
            return readParameters(new XMLFunction(XMLFunction.XMLTEXT));
        case "TRIM_ARRAY":
            return new ArrayFunction(readExpression(), readLastArgument(), null, ArrayFunction.TRIM_ARRAY);
        case "ARRAY_CONTAINS":
            return new ArrayFunction(readExpression(), readLastArgument(), null, ArrayFunction.ARRAY_CONTAINS);
        case "ARRAY_SLICE":
            return new ArrayFunction(readExpression(), readNextArgument(), readLastArgument(),
                    ArrayFunction.ARRAY_SLICE);
        case "COMPRESS":
            return new CompressFunction(readExpression(), readIfArgument(), CompressFunction.COMPRESS);
        case "EXPAND":
            return new CompressFunction(readSingleArgument(), null, CompressFunction.EXPAND);
        case "SOUNDEX":
            return new SoundexFunction(readSingleArgument(), null, SoundexFunction.SOUNDEX);
        case "DIFFERENCE":
            return new SoundexFunction(readExpression(), readLastArgument(), SoundexFunction.DIFFERENCE);
        case "JSON_OBJECT": {
            JsonConstructorFunction function = new JsonConstructorFunction(false);
            if (currentTokenType != CLOSE_PAREN && !readJsonObjectFunctionFlags(function, false)) {
                do {
                    boolean withKey = readIf(KEY);
                    function.addParameter(readExpression());
                    if (withKey) {
                        read(VALUE);
                    } else if (!readIf(VALUE)) {
                        read(COLON);
                    }
                    function.addParameter(readExpression());
                } while (readIf(COMMA));
                readJsonObjectFunctionFlags(function, false);
            }
            read(CLOSE_PAREN);
            function.doneWithParameters();
            return function;
        }
        case "JSON_ARRAY": {
            JsonConstructorFunction function = new JsonConstructorFunction(true);
            function.setFlags(JsonConstructorUtils.JSON_ABSENT_ON_NULL);
            if (currentTokenType != CLOSE_PAREN && !readJsonObjectFunctionFlags(function, true)) {
                do {
                    function.addParameter(readExpression());
                } while (readIf(COMMA));
                readJsonObjectFunctionFlags(function, true);
            }
            read(CLOSE_PAREN);
            function.doneWithParameters();
            return function;
        }
        case "ENCRYPT":
            return new CryptFunction(readExpression(), readNextArgument(), readLastArgument(), CryptFunction.ENCRYPT);
        case "DECRYPT":
            return new CryptFunction(readExpression(), readNextArgument(), readLastArgument(), CryptFunction.DECRYPT);
        case "COALESCE":
            return readCoalesceFunction(CoalesceFunction.COALESCE);
        case "GREATEST":
            return readCoalesceFunction(CoalesceFunction.GREATEST);
        case "LEAST":
            return readCoalesceFunction(CoalesceFunction.LEAST);
        case "NULLIF":
            return new NullIfFunction(readExpression(), readLastArgument());
        case "CONCAT":
            return readConcatFunction(ConcatFunction.CONCAT);
        case "CONCAT_WS":
            return readConcatFunction(ConcatFunction.CONCAT_WS);
        case "HASH":
            return new HashFunction(readExpression(), readNextArgument(), readIfArgument(), HashFunction.HASH);
        case "ORA_HASH": {
            Expression arg1 = readExpression();
            if (readIfMore()) {
                return new HashFunction(arg1, readExpression(), readIfArgument(), HashFunction.ORA_HASH);
            }
            return new HashFunction(arg1, HashFunction.ORA_HASH);
        }
        case "RAND":
        case "RANDOM":
            return new RandFunction(readIfSingleArgument(), RandFunction.RAND);
        case "SECURE_RAND":
            return new RandFunction(readSingleArgument(), RandFunction.SECURE_RAND);
        case "RANDOM_UUID":
        case "UUID":
            read(CLOSE_PAREN);
            return new RandFunction(null, RandFunction.RANDOM_UUID);
        case "ABORT_SESSION":
            return new SessionControlFunction(readIfSingleArgument(), SessionControlFunction.ABORT_SESSION);
        case "CANCEL_SESSION":
            return new SessionControlFunction(readIfSingleArgument(), SessionControlFunction.CANCEL_SESSION);
        case "AUTOCOMMIT":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.AUTOCOMMIT);
        case "DATABASE_PATH":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.DATABASE_PATH);
        case "H2VERSION":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.H2VERSION);
        case "LOCK_MODE":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.LOCK_MODE);
        case "LOCK_TIMEOUT":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.LOCK_TIMEOUT);
        case "MEMORY_FREE":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.MEMORY_FREE);
        case "MEMORY_USED":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.MEMORY_USED);
        case "READONLY":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.READONLY);
        case "SESSION_ID":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.SESSION_ID);
        case "TRANSACTION_ID":
            read(CLOSE_PAREN);
            return new SysInfoFunction(SysInfoFunction.TRANSACTION_ID);
        case "DISK_SPACE_USED":
            return new TableInfoFunction(readIfSingleArgument(), null, TableInfoFunction.DISK_SPACE_USED);
        case "ESTIMATED_ENVELOPE":
            return new TableInfoFunction(readExpression(), readLastArgument(), TableInfoFunction.ESTIMATED_ENVELOPE);
        case "FILE_READ":
            return new FileFunction(readExpression(), readIfArgument(), FileFunction.FILE_READ);
        case "FILE_WRITE":
            return new FileFunction(readExpression(), readLastArgument(), FileFunction.FILE_WRITE);
        case "DATA_TYPE_SQL":
            return new DataTypeSQLFunction(readExpression(), readNextArgument(), readNextArgument(),
                    readLastArgument());
        case "DB_OBJECT_ID":
            return new DBObjectFunction(readExpression(), readNextArgument(), readIfArgument(),
                    DBObjectFunction.DB_OBJECT_ID);
        case "DB_OBJECT_SQL":
            return new DBObjectFunction(readExpression(), readNextArgument(), readIfArgument(),
                    DBObjectFunction.DB_OBJECT_SQL);
        case "CSVWRITE":
            return readParameters(new CSVWriteFunction());
        case "SIGNAL":
            return new SignalFunction(readExpression(), readLastArgument());
        case "TRUNCATE_VALUE":
            return new TruncateValueFunction(readExpression(), readNextArgument(), readLastArgument());
        case "ZERO":
            read(CLOSE_PAREN);
            return ValueExpression.get(ValueInteger.get(0));
        case "PI":
            read(CLOSE_PAREN);
            return ValueExpression.get(ValueDouble.get(Math.PI));
        }
        ModeFunction function = ModeFunction.getFunction(database, upperName);
        return function != null ? readParameters(function) : null;
    }

    private Expression readDateTimeFormatFunction(int function) {
        DateTimeFormatFunction f = new DateTimeFormatFunction(function);
        f.addParameter(readExpression());
        read(COMMA);
        f.addParameter(readExpression());
        if (readIf(COMMA)) {
            f.addParameter(readExpression());
            if (readIf(COMMA)) {
                f.addParameter(readExpression());
            }
        }
        read(CLOSE_PAREN);
        f.doneWithParameters();
        return f;
    }

    private Expression readTrimFunction() {
        int flags;
        boolean needFrom = false;
        if (readIf("LEADING")) {
            flags = TrimFunction.LEADING;
            needFrom = true;
        } else if (readIf("TRAILING")) {
            flags = TrimFunction.TRAILING;
            needFrom = true;
        } else {
            needFrom = readIf("BOTH");
            flags = TrimFunction.LEADING | TrimFunction.TRAILING;
        }
        Expression from, space = null;
        if (needFrom) {
            if (!readIf(FROM)) {
                space = readExpression();
                read(FROM);
            }
            from = readExpression();
        } else {
            if (readIf(FROM)) {
                from = readExpression();
            } else {
                from = readExpression();
                if (readIf(FROM)) {
                    space = from;
                    from = readExpression();
                } else if (readIf(COMMA)) {
                    space = readExpression();
                }
            }
        }
        read(CLOSE_PAREN);
        return new TrimFunction(from, space, flags);
    }

    private ArrayTableFunction readUnnestFunction() {
        ArrayTableFunction f = new ArrayTableFunction(ArrayTableFunction.UNNEST);
        ArrayList<Column> columns = Utils.newSmallArrayList();
        if (!readIf(CLOSE_PAREN)) {
            int i = 0;
            do {
                Expression expr = readExpression();
                TypeInfo columnType = TypeInfo.TYPE_NULL;
                if (expr.isConstant()) {
                    expr = expr.optimize(session);
                    TypeInfo exprType = expr.getType();
                    if (exprType.getValueType() == Value.ARRAY) {
                        columnType = (TypeInfo) exprType.getExtTypeInfo();
                    }
                }
                f.addParameter(expr);
                columns.add(new Column("C" + ++i, columnType));
            } while (readIfMore());
        }
        if (readIf(WITH)) {
            read("ORDINALITY");
            columns.add(new Column("NORD", TypeInfo.TYPE_INTEGER));
        }
        f.setColumns(columns);
        f.doneWithParameters();
        return f;
    }

    private ArrayTableFunction readTableFunction(int functionType) {
        ArrayTableFunction f = new ArrayTableFunction(functionType);
        ArrayList<Column> columns = Utils.newSmallArrayList();
        do {
            columns.add(parseColumnWithType(readIdentifier()));
            read(EQUAL);
            f.addParameter(readExpression());
        } while (readIfMore());
        f.setColumns(columns);
        f.doneWithParameters();
        return f;
    }

    private Expression readSingleArgument() {
        Expression arg = readExpression();
        read(CLOSE_PAREN);
        return arg;
    }

    private Expression readNextArgument() {
        read(COMMA);
        return readExpression();
    }

    private Expression readLastArgument() {
        read(COMMA);
        Expression arg = readExpression();
        read(CLOSE_PAREN);
        return arg;
    }

    private Expression readIfSingleArgument() {
        Expression arg;
        if (readIf(CLOSE_PAREN)) {
            arg = null;
        } else {
            arg = readExpression();
            read(CLOSE_PAREN);
        }
        return arg;
    }

    private Expression readIfArgument() {
        Expression arg = readIf(COMMA) ? readExpression() : null;
        read(CLOSE_PAREN);
        return arg;
    }

    private Expression readCoalesceFunction(int function) {
        CoalesceFunction f = new CoalesceFunction(function);
        f.addParameter(readExpression());
        while (readIfMore()) {
            f.addParameter(readExpression());
        }
        f.doneWithParameters();
        return f;
    }

    private Expression readConcatFunction(int function) {
        ConcatFunction f = new ConcatFunction(function);
        f.addParameter(readExpression());
        f.addParameter(readNextArgument());
        if (function == ConcatFunction.CONCAT_WS) {
            f.addParameter(readNextArgument());
        }
        while (readIfMore()) {
            f.addParameter(readExpression());
        }
        f.doneWithParameters();
        return f;
    }

    private Expression readSubstringFunction() {
        // Standard variants are:
        // SUBSTRING(X FROM 1)
        // SUBSTRING(X FROM 1 FOR 1)
        // Different non-standard variants include:
        // SUBSTRING(X,1)
        // SUBSTRING(X,1,1)
        // SUBSTRING(X FOR 1) -- Postgres
        SubstringFunction function = new SubstringFunction();
        function.addParameter(readExpression());
        if (readIf(FROM)) {
            function.addParameter(readExpression());
            if (readIf(FOR)) {
                function.addParameter(readExpression());
            }
        } else if (readIf(FOR)) {
            function.addParameter(ValueExpression.get(ValueInteger.get(1)));
            function.addParameter(readExpression());
        } else {
            read(COMMA);
            function.addParameter(readExpression());
            if (readIf(COMMA)) {
                function.addParameter(readExpression());
            }
        }
        read(CLOSE_PAREN);
        function.doneWithParameters();
        return function;
    }

    private int readDateTimeField() {
        int field = -1;
        switch (currentTokenType) {
        case IDENTIFIER:
            if (!currentTokenQuoted) {
                field = DateTimeFunction.getField(currentToken);
            }
            break;
        case LITERAL:
            if (currentValue.getValueType() == Value.VARCHAR) {
                field = DateTimeFunction.getField(currentValue.getString());
            }
            break;
        case YEAR:
            field = DateTimeFunction.YEAR;
            break;
        case MONTH:
            field = DateTimeFunction.MONTH;
            break;
        case DAY:
            field = DateTimeFunction.DAY;
            break;
        case HOUR:
            field = DateTimeFunction.HOUR;
            break;
        case MINUTE:
            field = DateTimeFunction.MINUTE;
            break;
        case SECOND:
            field = DateTimeFunction.SECOND;
        }
        if (field < 0) {
            addExpected("date-time field");
            throw getSyntaxError();
        }
        read();
        return field;
    }

    private WindowFunction readWindowFunction(String name) {
        WindowFunctionType type = WindowFunctionType.get(name);
        if (type == null) {
            return null;
        }
        if (currentSelect == null) {
            throw getSyntaxError();
        }
        int numArgs = WindowFunction.getMinArgumentCount(type);
        Expression[] args = null;
        if (numArgs > 0) {
            // There is no functions with numArgs == 0 && numArgsMax > 0
            int numArgsMax = WindowFunction.getMaxArgumentCount(type);
            args = new Expression[numArgsMax];
            if (numArgs == numArgsMax) {
                for (int i = 0; i < numArgs; i++) {
                    if (i > 0) {
                        read(COMMA);
                    }
                    args[i] = readExpression();
                }
            } else {
                int i = 0;
                while (i < numArgsMax) {
                    if (i > 0 && !readIf(COMMA)) {
                        break;
                    }
                    args[i] = readExpression();
                    i++;
                }
                if (i < numArgs) {
                    throw getSyntaxError();
                }
                if (i != numArgsMax) {
                    args = Arrays.copyOf(args, i);
                }
            }
        }
        read(CLOSE_PAREN);
        WindowFunction function = new WindowFunction(type, currentSelect, args);
        switch (type) {
        case NTH_VALUE:
            readFromFirstOrLast(function);
            //$FALL-THROUGH$
        case LEAD:
        case LAG:
        case FIRST_VALUE:
        case LAST_VALUE:
            readRespectOrIgnoreNulls(function);
            //$FALL-THROUGH$
        default:
            // Avoid warning
        }
        readOver(function);
        return function;
    }

    private void readFromFirstOrLast(WindowFunction function) {
        if (readIf(FROM) && !readIf("FIRST")) {
            read("LAST");
            function.setFromLast(true);
        }
    }

    private void readRespectOrIgnoreNulls(WindowFunction function) {
        if (readIf("RESPECT")) {
            read("NULLS");
        } else if (readIf("IGNORE")) {
            read("NULLS");
            function.setIgnoreNulls(true);
        }
    }

    private boolean readJsonObjectFunctionFlags(ExpressionWithFlags function, boolean forArray) {
        int start = lastParseIndex;
        boolean result = false;
        int flags = function.getFlags();
        if (readIf(NULL)) {
            if (readIf(ON)) {
                read(NULL);
                flags &= ~JsonConstructorUtils.JSON_ABSENT_ON_NULL;
                result = true;
            } else {
                reread(start);
                return false;
            }
        } else if (readIf("ABSENT")) {
            if (readIf(ON)) {
                read(NULL);
                flags |= JsonConstructorUtils.JSON_ABSENT_ON_NULL;
                result = true;
            } else {
                reread(start);
                return false;
            }
        }
        if (!forArray) {
            if (readIf(WITH)) {
                read(UNIQUE);
                read("KEYS");
                flags |= JsonConstructorUtils.JSON_WITH_UNIQUE_KEYS;
                result = true;
            } else if (readIf("WITHOUT")) {
                if (readIf(UNIQUE)) {
                    read("KEYS");
                    flags &= ~JsonConstructorUtils.JSON_WITH_UNIQUE_KEYS;
                    result = true;
                } else if (result) {
                    throw getSyntaxError();
                } else {
                    reread(start);
                    return false;
                }
            }
        }
        if (result) {
            function.setFlags(flags);
        }
        return result;
    }

    private Expression readKeywordCompatibilityFunctionOrColumn() {
        boolean nonKeyword = nonKeywords != null && nonKeywords.get(currentTokenType);
        String name = currentToken;
        read();
        if (readIf(OPEN_PAREN)) {
            return readCompatibilityFunction(upperName(name));
        } else if (nonKeyword) {
            return readIf(DOT) ? readTermObjectDot(name) : new ExpressionColumn(database, null, null, name);
        }
        throw getSyntaxError();
    }

    private Expression readCurrentDateTimeValueFunction(int function, boolean hasParen, String name) {
        int scale = -1;
        if (hasParen) {
            if (function != CurrentDateTimeValueFunction.CURRENT_DATE && currentTokenType != CLOSE_PAREN) {
                scale = readInt();
                if (scale < 0 || scale > ValueTime.MAXIMUM_SCALE) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale), "0",
                            /* compile-time constant */ "" + ValueTime.MAXIMUM_SCALE);
                }
            }
            read(CLOSE_PAREN);
        }
        if (database.isAllowBuiltinAliasOverride()) {
            FunctionAlias functionAlias = database.getSchema(session.getCurrentSchemaName())
                    .findFunction(name != null ? name : CurrentDateTimeValueFunction.getName(function));
            if (functionAlias != null) {
                return new JavaFunction(functionAlias,
                        scale >= 0 ? new Expression[] { ValueExpression.get(ValueInteger.get(scale)) }
                                : new Expression[0]);
            }
        }
        return new CurrentDateTimeValueFunction(function, scale);
    }

    private Expression readIfWildcardRowidOrSequencePseudoColumn(String schema, String objectName) {
        if (readIf(ASTERISK)) {
            return parseWildcard(schema, objectName);
        }
        if (readIf(_ROWID_)) {
            return new ExpressionColumn(database, schema, objectName);
        }
        if (database.getMode().nextvalAndCurrvalPseudoColumns) {
            return readIfSequencePseudoColumn(schema, objectName);
        }
        return null;
    }

    private Wildcard parseWildcard(String schema, String objectName) {
        Wildcard wildcard = new Wildcard(schema, objectName);
        if (readIf(EXCEPT)) {
            read(OPEN_PAREN);
            ArrayList<ExpressionColumn> exceptColumns = Utils.newSmallArrayList();
            do {
                String s = null, t = null;
                String name = readIdentifier();
                if (readIf(DOT)) {
                    t = name;
                    name = readIdentifier();
                    if (readIf(DOT)) {
                        s = t;
                        t = name;
                        name = readIdentifier();
                        if (readIf(DOT)) {
                            checkDatabaseName(s);
                            s = t;
                            t = name;
                            name = readIdentifier();
                        }
                    }
                }
                exceptColumns.add(new ExpressionColumn(database, s, t, name));
            } while (readIfMore());
            wildcard.setExceptColumns(exceptColumns);
        }
        return wildcard;
    }

    private SequenceValue readIfSequencePseudoColumn(String schema, String objectName) {
        if (schema == null) {
            schema = session.getCurrentSchemaName();
        }
        if (isToken("NEXTVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                read();
                return new SequenceValue(sequence, getCurrentSelectOrPrepared());
            }
        } else if (isToken("CURRVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                read();
                return new SequenceValue(sequence);
            }
        }
        return null;
    }

    private Expression readTermObjectDot(String objectName) {
        Expression expr = readIfWildcardRowidOrSequencePseudoColumn(null, objectName);
        if (expr != null) {
            return expr;
        }
        String name = readIdentifier();
        if (readIf(OPEN_PAREN)) {
            return readFunction(database.getSchema(objectName), name);
        } else if (readIf(DOT)) {
            String schema = objectName;
            objectName = name;
            expr = readIfWildcardRowidOrSequencePseudoColumn(schema, objectName);
            if (expr != null) {
                return expr;
            }
            name = readIdentifier();
            if (readIf(OPEN_PAREN)) {
                checkDatabaseName(schema);
                return readFunction(database.getSchema(objectName), name);
            } else if (readIf(DOT)) {
                checkDatabaseName(schema);
                schema = objectName;
                objectName = name;
                expr = readIfWildcardRowidOrSequencePseudoColumn(schema, objectName);
                if (expr != null) {
                    return expr;
                }
                name = readIdentifier();
            }
            return new ExpressionColumn(database, schema, objectName, name);
        }
        return new ExpressionColumn(database, null, objectName, name);
    }

    private void checkDatabaseName(String databaseName) {
        if (!database.getIgnoreCatalogs() && !equalsToken(database.getShortName(), databaseName)) {
            throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, databaseName);
        }
    }

    private Parameter readParameter() {
        // there must be no space between ? and the number
        boolean indexed = Character.isDigit(sqlCommandChars[parseIndex]);

        Parameter p;
        if (indexed) {
            readParameterIndex();
            if (indexedParameterList == null) {
                if (parameters == null) {
                    // this can occur when parsing expressions only (for
                    // example check constraints)
                    throw getSyntaxError();
                } else if (!parameters.isEmpty()) {
                    throw DbException
                            .get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                }
                indexedParameterList = Utils.newSmallArrayList();
            }
            int index = currentValue.getInt() - 1;
            if (index < 0 || index >= Constants.MAX_PARAMETER_INDEX) {
                throw DbException.getInvalidValueException(
                        "parameter index", index + 1);
            }
            if (indexedParameterList.size() <= index) {
                indexedParameterList.ensureCapacity(index + 1);
                while (indexedParameterList.size() <= index) {
                    indexedParameterList.add(null);
                }
            }
            p = indexedParameterList.get(index);
            if (p == null) {
                p = new Parameter(index);
                indexedParameterList.set(index, p);
                parameters.add(p);
            }
            read();
        } else {
            read();
            if (indexedParameterList != null) {
                throw DbException
                        .get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
            }
            p = new Parameter(parameters.size());
            parameters.add(p);
        }
        return p;
    }

    private Expression readTerm() {
        Expression r;
        switch (currentTokenType) {
        case AT:
            read();
            r = new Variable(session, readIdentifier());
            if (readIf(COLON_EQ)) {
                r = new SetFunction(r, readExpression());
            }
            break;
        case PARAMETER:
            r = readParameter();
            break;
        case TABLE:
        case SELECT:
        case WITH:
            r = new Subquery(parseQuery());
            break;
        case MINUS_SIGN:
            read();
            if (currentTokenType == LITERAL) {
                r = ValueExpression.get(currentValue.negate());
                int rType = r.getType().getValueType();
                if (rType == Value.BIGINT &&
                        r.getValue(session).getLong() == Integer.MIN_VALUE) {
                    // convert Integer.MIN_VALUE to type 'int'
                    // (Integer.MAX_VALUE+1 is of type 'long')
                    r = ValueExpression.get(ValueInteger.get(Integer.MIN_VALUE));
                } else if (rType == Value.NUMERIC &&
                        r.getValue(session).getBigDecimal().compareTo(Value.MIN_LONG_DECIMAL) == 0) {
                    // convert Long.MIN_VALUE to type 'long'
                    // (Long.MAX_VALUE+1 is of type 'decimal')
                    r = ValueExpression.get(ValueBigint.MIN);
                }
                read();
            } else {
                r = new UnaryOperation(readTerm());
            }
            break;
        case PLUS_SIGN:
            read();
            r = readTerm();
            break;
        case OPEN_PAREN:
            read();
            if (readIf(CLOSE_PAREN)) {
                r = ValueExpression.get(ValueRow.EMPTY);
            } else {
                r = readExpression();
                if (readIfMore()) {
                    ArrayList<Expression> list = Utils.newSmallArrayList();
                    list.add(r);
                    do {
                        list.add(readExpression());
                    } while (readIfMore());
                    r = new ExpressionList(list.toArray(new Expression[0]), false);
                } else if (r instanceof BinaryOperation) {
                    BinaryOperation binaryOperation = (BinaryOperation) r;
                    if (binaryOperation.getOperationType() == OpType.MINUS) {
                        TypeInfo ti = readIntervalQualifier();
                        if (ti != null) {
                            binaryOperation.setForcedType(ti);
                        }
                    }
                }
            }
            if (readIf(DOT)) {
                r = new FieldReference(r, readIdentifier());
            }
            break;
        case ARRAY:
            read();
            if (readIf(OPEN_BRACKET)) {
                if (readIf(CLOSE_BRACKET)) {
                    r = ValueExpression.get(ValueArray.EMPTY);
                } else {
                    ArrayList<Expression> list = Utils.newSmallArrayList();
                    do {
                        list.add(readExpression());
                    } while (readIf(COMMA));
                    read(CLOSE_BRACKET);
                    r = new ExpressionList(list.toArray(new Expression[0]), true);
                }
            } else {
                read(OPEN_PAREN);
                Query q = parseQuery();
                read(CLOSE_PAREN);
                r = new ArrayConstructorByQuery(q);
            }
            break;
        case INTERVAL:
            read();
            r = readInterval();
            break;
        case ROW: {
            read();
            read(OPEN_PAREN);
            if (readIf(CLOSE_PAREN)) {
                r = ValueExpression.get(ValueRow.EMPTY);
            } else {
                ArrayList<Expression> list = Utils.newSmallArrayList();
                do {
                    list.add(readExpression());
                } while (readIfMore());
                r = new ExpressionList(list.toArray(new Expression[0]), false);
            }
            break;
        }
        case TRUE:
            read();
            r = ValueExpression.TRUE;
            break;
        case FALSE:
            read();
            r = ValueExpression.FALSE;
            break;
        case UNKNOWN:
            read();
            r = TypedValueExpression.UNKNOWN;
            break;
        case ROWNUM:
            read();
            if (readIf(OPEN_PAREN)) {
                read(CLOSE_PAREN);
            }
            if (currentSelect == null && currentPrepared == null) {
                throw getSyntaxError();
            }
            r = new Rownum(getCurrentSelectOrPrepared());
            break;
        case NULL:
            read();
            r = ValueExpression.NULL;
            break;
        case _ROWID_:
            read();
            r = new ExpressionColumn(database, null, null);
            break;
        case LITERAL:
            r = ValueExpression.get(currentValue);
            read();
            break;
        case VALUES:
            if (database.getMode().onDuplicateKeyUpdate) {
                if (currentPrepared instanceof Insert) {
                    r = readOnDuplicateKeyValues(((Insert) currentPrepared).getTable(), null);
                    break;
                } else if (currentPrepared instanceof Update) {
                    Update update = (Update) currentPrepared;
                    r = readOnDuplicateKeyValues(update.getTable(), update);
                    break;
                }
            }
            r = new Subquery(parseQuery());
            break;
        case CASE:
            read();
            r = readCase();
            break;
        case CAST: {
            read();
            read(OPEN_PAREN);
            Expression arg = readExpression();
            read(AS);
            Column column = parseColumnWithType(null);
            read(CLOSE_PAREN);
            r = new CastSpecification(arg, column);
            break;
        }
        case CURRENT_CATALOG:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_CATALOG);
        case CURRENT_DATE:
            read();
            r = readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_DATE, readIf(OPEN_PAREN), null);
            break;
        case CURRENT_PATH:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_PATH);
        case CURRENT_ROLE:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_ROLE);
        case CURRENT_SCHEMA:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_SCHEMA);
        case CURRENT_TIME:
            read();
            r = readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_TIME, readIf(OPEN_PAREN), null);
            break;
        case CURRENT_TIMESTAMP:
            read();
            r = readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_TIMESTAMP, readIf(OPEN_PAREN),
                    null);
            break;
        case CURRENT_USER:
        case USER:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.CURRENT_USER);
        case SESSION_USER:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.SESSION_USER);
        case SYSTEM_USER:
            return readCurrentGeneralValueSpecification(CurrentGeneralValueSpecification.SYSTEM_USER);
        case ANY:
        case SOME:
            read();
            read(OPEN_PAREN);
            return readAggregate(AggregateType.ANY, "ANY");
        case DAY:
        case HOUR:
        case MINUTE:
        case MONTH:
        case SECOND:
        case YEAR:
            r = readKeywordCompatibilityFunctionOrColumn();
            break;
        case LEFT:
            r = readColumnIfNotFunction();
            if (r == null) {
                r = new StringFunction2(readExpression(), readLastArgument(), StringFunction2.LEFT);
            }
            break;
        case LOCALTIME:
            read();
            r = readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIME, readIf(OPEN_PAREN), null);
            break;
        case LOCALTIMESTAMP:
            read();
            r = readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, readIf(OPEN_PAREN), //
                    null);
            break;
        case RIGHT:
            r = readColumnIfNotFunction();
            if (r == null) {
                r = new StringFunction2(readExpression(), readLastArgument(), StringFunction2.RIGHT);
            }
            break;
        case SET:
            r = readColumnIfNotFunction();
            if (r == null) {
                r = readSetFunction();
            }
            break;
        case VALUE:
            if (parseDomainConstraint) {
                read();
                r = new DomainValueExpression();
                break;
            }
            //$FALL-THROUGH$
        default:
            if (!isIdentifier()) {
                throw getSyntaxError();
            }
            //$FALL-THROUGH$
        case IDENTIFIER:
            String name = currentToken;
            boolean quoted = currentTokenQuoted;
            read();
            if (readIf(OPEN_PAREN)) {
                r = readFunction(null, name);
            } else if (readIf(DOT)) {
                r = readTermObjectDot(name);
            } else if (quoted) {
                r = new ExpressionColumn(database, null, null, name);
            } else {
                r = readTermWithIdentifier(name, quoted);
            }
            break;
        }
        if (readIf(OPEN_BRACKET)) {
            r = new ArrayElementReference(r, readExpression());
            read(CLOSE_BRACKET);
        }
        colonColon: if (readIf(COLON_COLON)) {
            if (database.getMode().getEnum() == ModeEnum.PostgreSQL) {
                // PostgreSQL compatibility
                if (isToken("PG_CATALOG")) {
                    read("PG_CATALOG");
                    read(DOT);
                }
                if (readIf("REGCLASS")) {
                    r = new Regclass(r);
                    break colonColon;
                }
            }
            r = new CastSpecification(r, parseColumnWithType(null));
        }
        for (;;) {
            TypeInfo ti = readIntervalQualifier();
            if (ti != null) {
                r = new CastSpecification(r, ti);
            }
            int index = lastParseIndex;
            if (readIf("AT")) {
                if (readIf("TIME")) {
                    read("ZONE");
                    r = new TimeZoneOperation(r, readExpression());
                    continue;
                } else if (readIf("LOCAL")) {
                    r = new TimeZoneOperation(r, null);
                    continue;
                } else {
                    reread(index);
                }
            } else if (readIf("FORMAT")) {
                if (readIf("JSON")) {
                    r = new Format(r, FormatEnum.JSON);
                    continue;
                } else {
                    reread(index);
                }
            }
            break;
        }
        return r;
    }

    private Expression readCurrentGeneralValueSpecification(int specification) {
        read();
        if (readIf(OPEN_PAREN)) {
            read(CLOSE_PAREN);
        }
        return new CurrentGeneralValueSpecification(specification);
    }

    private Expression readColumnIfNotFunction() {
        boolean nonKeyword = nonKeywords != null && nonKeywords.get(currentTokenType);
        String name = currentToken;
        read();
        if (readIf(OPEN_PAREN)) {
            return null;
        } else if (nonKeyword) {
            return readIf(DOT) ? readTermObjectDot(name) : new ExpressionColumn(database, null, null, name);
        }
        throw getSyntaxError();
    }

    private Expression readSetFunction() {
        SetFunction function = new SetFunction(readExpression(), readLastArgument());
        if (database.isAllowBuiltinAliasOverride()) {
            FunctionAlias functionAlias = database.getSchema(session.getCurrentSchemaName()).findFunction(
                    function.getName());
            if (functionAlias != null) {
                return new JavaFunction(functionAlias,
                        new Expression[] { function.getSubexpression(0), function.getSubexpression(1) });
            }
        }
        return function;
    }

    private Expression readOnDuplicateKeyValues(Table table, Update update) {
        read();
        read(OPEN_PAREN);
        Column c = readTableColumn(new TableFilter(session, table, null, rightsChecked, null, 0, null));
        read(CLOSE_PAREN);
        return new OnDuplicateKeyValues(c, update);
    }

    private Expression readTermWithIdentifier(String name, boolean quoted) {
        /*
         * Convert a-z to A-Z. This method is safe, because only A-Z
         * characters are considered below.
         *
         * Unquoted identifier is never empty.
         */
        switch (name.charAt(0) & 0xffdf) {
        case 'C':
            if (equalsToken("CURRENT", name)) {
                int index = lastParseIndex;
                if (readIf(VALUE) && readIf(FOR)) {
                    return new SequenceValue(readSequence());
                }
                reread(index);
                if (database.getMode().getEnum() == ModeEnum.DB2) {
                    return parseDB2SpecialRegisters(name);
                }
            }
            break;
        case 'D':
            if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR &&
                    (equalsToken("DATE", name) || equalsToken("D", name))) {
                String date = currentValue.getString();
                read();
                return ValueExpression.get(ValueDate.parse(date));
            }
            break;
        case 'E':
            if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR //
                    && equalsToken("E", name)) {
                String text = currentValue.getString();
                // the PostgreSQL ODBC driver uses
                // LIKE E'PROJECT\\_DATA' instead of LIKE
                // 'PROJECT\_DATA'
                // N: SQL-92 "National Language" strings
                text = StringUtils.replaceAll(text, "\\\\", "\\");
                read();
                return ValueExpression.get(ValueVarchar.get(text));
            }
            break;
        case 'G':
            if (currentTokenType == LITERAL) {
                int t = currentValue.getValueType();
                if (t == Value.VARCHAR && equalsToken("GEOMETRY", name)) {
                    ValueExpression v = ValueExpression.get(ValueGeometry.get(currentValue.getString()));
                    read();
                    return v;
                } else if (t == Value.VARBINARY && equalsToken("GEOMETRY", name)) {
                    ValueExpression v = ValueExpression.get(ValueGeometry.getFromEWKB(currentValue.getBytesNoCopy()));
                    read();
                    return v;
                }
            }
            break;
        case 'J':
            if (currentTokenType == LITERAL) {
                int t = currentValue.getValueType();
                if (t == Value.VARCHAR && equalsToken("JSON", name)) {
                    ValueExpression v = ValueExpression.get(ValueJson.fromJson(currentValue.getString()));
                    read();
                    return v;
                } else if (t == Value.VARBINARY && equalsToken("JSON", name)) {
                    ValueExpression v = ValueExpression.get(ValueJson.fromJson(currentValue.getBytesNoCopy()));
                    read();
                    return v;
                }
            }
            break;
        case 'N':
            if (equalsToken("NEXT", name)) {
                int index = lastParseIndex;
                if (readIf(VALUE) && readIf(FOR)) {
                    return new SequenceValue(readSequence(), getCurrentSelectOrPrepared());
                }
                reread(index);
            }
            break;
        case 'T':
            if (equalsToken("TIME", name)) {
                if (readIf(WITH)) {
                    read("TIME");
                    read("ZONE");
                    if (currentTokenType != LITERAL || currentValue.getValueType() != Value.VARCHAR) {
                        throw getSyntaxError();
                    }
                    String time = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTimeTimeZone.parse(time));
                } else {
                    boolean without = readIf("WITHOUT");
                    if (without) {
                        read("TIME");
                        read("ZONE");
                    }
                    if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR) {
                        String time = currentValue.getString();
                        read();
                        return ValueExpression.get(ValueTime.parse(time));
                    } else if (without) {
                        throw getSyntaxError();
                    }
                }
            } else if (equalsToken("TIMESTAMP", name)) {
                if (readIf(WITH)) {
                    read("TIME");
                    read("ZONE");
                    if (currentTokenType != LITERAL || currentValue.getValueType() != Value.VARCHAR) {
                        throw getSyntaxError();
                    }
                    String timestamp = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTimestampTimeZone.parse(timestamp, session));
                } else {
                    boolean without = readIf("WITHOUT");
                    if (without) {
                        read("TIME");
                        read("ZONE");
                    }
                    if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR) {
                        String timestamp = currentValue.getString();
                        read();
                        return ValueExpression.get(ValueTimestamp.parse(timestamp, session));
                    } else if (without) {
                        throw getSyntaxError();
                    }
                }
            } else if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR) {
                if (equalsToken("T", name)) {
                    String time = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTime.parse(time));
                } else if (equalsToken("TS", name)) {
                    String timestamp = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTimestamp.parse(timestamp, session));
                }
            }
            break;
        case 'U':
            if (currentTokenType == LITERAL && currentValue.getValueType() == Value.VARCHAR
                    && (equalsToken("UUID", name))) {
                String uuid = currentValue.getString();
                read();
                return ValueExpression.get(ValueUuid.get(uuid));
            }
            break;
        }
        return new ExpressionColumn(database, null, null, name, quoted);
    }

    private Prepared getCurrentSelectOrPrepared() {
        return currentSelect == null ? currentPrepared : currentSelect;
    }

    private Expression readInterval() {
        boolean negative = readIf(MINUS_SIGN);
        if (!negative) {
            readIf(PLUS_SIGN);
        }
        if (currentTokenType != LITERAL || currentValue.getValueType() != Value.VARCHAR) {
            addExpected("string");
            throw getSyntaxError();
        }
        String s = currentValue.getString();
        read();
        IntervalQualifier qualifier;
        switch (currentTokenType) {
        case YEAR:
            read();
            if (readIf(TO)) {
                read(MONTH);
                qualifier = IntervalQualifier.YEAR_TO_MONTH;
            } else {
                qualifier = IntervalQualifier.YEAR;
            }
            break;
        case MONTH:
            read();
            qualifier = IntervalQualifier.MONTH;
            break;
        case DAY:
            read();
            if (readIf(TO)) {
                switch (currentTokenType) {
                case HOUR:
                    qualifier = IntervalQualifier.DAY_TO_HOUR;
                    break;
                case MINUTE:
                    qualifier = IntervalQualifier.DAY_TO_MINUTE;
                    break;
                case SECOND:
                    qualifier = IntervalQualifier.DAY_TO_SECOND;
                    break;
                default:
                    throw intervalDayError();
                }
                read();
            } else {
                qualifier = IntervalQualifier.DAY;
            }
            break;
        case HOUR:
            read();
            if (readIf(TO)) {
                switch (currentTokenType) {
                case MINUTE:
                    qualifier = IntervalQualifier.HOUR_TO_MINUTE;
                    break;
                case SECOND:
                    qualifier = IntervalQualifier.HOUR_TO_SECOND;
                    break;
                default:
                    throw intervalHourError();
                }
                read();
            } else {
                qualifier = IntervalQualifier.HOUR;
            }
            break;
        case MINUTE:
            read();
            if (readIf(TO)) {
                read(SECOND);
                qualifier = IntervalQualifier.MINUTE_TO_SECOND;
            } else {
                qualifier = IntervalQualifier.MINUTE;
            }
            break;
        case SECOND:
            read();
            qualifier = IntervalQualifier.SECOND;
            break;
        default:
            throw intervalQualifierError();
        }
        try {
            return ValueExpression.get(IntervalUtils.parseInterval(qualifier, negative, s));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
        }
    }

    private Expression parseDB2SpecialRegisters(String name) {
        // Only "CURRENT" name is supported
        if (readIf("TIMESTAMP")) {
            if (readIf(WITH)) {
                read("TIME");
                read("ZONE");
                return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_TIMESTAMP,
                        readIf(OPEN_PAREN), null);
            }
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, readIf(OPEN_PAREN),
                    null);
        } else if (readIf("TIME")) {
            // Time with fractional seconds is not supported by DB2
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIME, false, null);
        } else if (readIf("DATE")) {
            return readCurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_DATE, false, null);
        }
        // No match, parse CURRENT as a column
        return new ExpressionColumn(database, null, null, name);
    }

    private Expression readCase() {
        Expression c;
        if (readIf(WHEN)) {
            SearchedCase searched = new SearchedCase();
            do {
                Expression condition = readExpression();
                read("THEN");
                searched.addParameter(condition);
                searched.addParameter(readExpression());
            } while (readIf(WHEN));
            if (readIf(ELSE)) {
                searched.addParameter(readExpression());
            }
            searched.doneWithParameters();
            c = searched;
        } else {
            Expression caseOperand = readExpression();
            read(WHEN);
            SimpleCase.SimpleWhen when = readSimpleWhenClause(caseOperand), current = when;
            while (readIf(WHEN)) {
                SimpleCase.SimpleWhen next = readSimpleWhenClause(caseOperand);
                current.setWhen(next);
                current = next;
            }
            c = new SimpleCase(caseOperand, when, readIf(ELSE) ? readExpression() : null);
        }
        read(END);
        return c;
    }

    private SimpleCase.SimpleWhen readSimpleWhenClause(Expression caseOperand) {
        Expression whenOperand = readWhenOperand(caseOperand);
        if (readIf(COMMA)) {
            ArrayList<Expression> operands = Utils.newSmallArrayList();
            operands.add(whenOperand);
            do {
                operands.add(readWhenOperand(caseOperand));
            } while (readIf(COMMA));
            read("THEN");
            return new SimpleCase.SimpleWhen(operands.toArray(new Expression[0]), readExpression());
        }
        read("THEN");
        return new SimpleCase.SimpleWhen(whenOperand, readExpression());
    }

    private Expression readWhenOperand(Expression caseOperand) {
        int backup = parseIndex;
        boolean not = readIf(NOT);
        Expression whenOperand = readConditionRightHandSide(caseOperand, not, true);
        if (whenOperand == null) {
            if (not) {
                parseIndex = backup;
                currentToken = "NOT";
                currentTokenType = NOT;
            }
            whenOperand = readExpression();
        }
        return whenOperand;
    }

    private int readNonNegativeInt() {
        int v = readInt();
        if (v < 0) {
            throw DbException.getInvalidValueException("non-negative integer", v);
        }
        return v;
    }

    private int readInt() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != LITERAL) {
            throw DbException.getSyntaxError(sqlCommand, parseIndex, "integer");
        }
        if (minus) {
            // must do that now, otherwise Integer.MIN_VALUE would not work
            currentValue = currentValue.negate();
        }
        int i = currentValue.getInt();
        read();
        return i;
    }

    private long readPositiveLong() {
        long v = readLong();
        if (v <= 0) {
            throw DbException.getInvalidValueException("positive long", v);
        }
        return v;
    }

    private long readLong() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != LITERAL) {
            throw DbException.getSyntaxError(sqlCommand, parseIndex, "long");
        }
        if (minus) {
            // must do that now, otherwise Long.MIN_VALUE would not work
            currentValue = currentValue.negate();
        }
        long i = currentValue.getLong();
        read();
        return i;
    }

    private boolean readBooleanSetting() {
        switch (currentTokenType) {
        case ON:
        case TRUE:
            read();
            return true;
        case FALSE:
            read();
            return false;
        case LITERAL:
            boolean result = currentValue.getBoolean();
            read();
            return result;
        }
        if (readIf("OFF")) {
            return false;
        } else {
            if (expectedList != null) {
                addMultipleExpected(ON, TRUE, FALSE);
            }
            throw getSyntaxError();
        }
    }

    private String readString() {
        int index = parseIndex;
        Expression expr = readExpression();
        try {
            String s = expr.optimize(session).getValue(session).getString();
            if (s == null || s.length() <= Constants.MAX_STRING_LENGTH) {
                return s;
            }
        } catch (DbException e) {
        }
        throw DbException.getSyntaxError(sqlCommand, index, "character string");
    }

    // TODO: why does this function allow defaultSchemaName=null - which resets
    // the parser schemaName for everyone ?
    private String readIdentifierWithSchema(String defaultSchemaName) {
        String s = readIdentifier();
        schemaName = defaultSchemaName;
        if (readIf(DOT)) {
            s = readIdentifierWithSchema2(s);
        }
        return s;
    }

    private String readIdentifierWithSchema2(String s) {
        schemaName = s;
        if (database.getMode().allowEmptySchemaValuesAsDefaultSchema && readIf(DOT)) {
            if (equalsToken(schemaName, database.getShortName()) || database.getIgnoreCatalogs()) {
                schemaName = session.getCurrentSchemaName();
                s = readIdentifier();
            }
        } else {
            s = readIdentifier();
            if (currentTokenType == DOT) {
                if (equalsToken(schemaName, database.getShortName()) || database.getIgnoreCatalogs()) {
                    read();
                    schemaName = s;
                    s = readIdentifier();
                }
            }
        }
        return s;
    }

    private String readIdentifierWithSchema() {
        return readIdentifierWithSchema(session.getCurrentSchemaName());
    }

    private String readIdentifier() {
        if (!isIdentifier()) {
            /*
             * Sometimes a new keywords are introduced. During metadata
             * initialization phase keywords are accepted as identifiers to
             * allow migration from older versions.
             */
            if (!session.isQuirksMode() || !isKeyword(currentTokenType)) {
                throw DbException.getSyntaxError(sqlCommand, parseIndex, "identifier");
            }
        }
        String s = currentToken;
        read();
        return s;
    }

    private void read(String expected) {
        if (currentTokenQuoted || !equalsToken(expected, currentToken)) {
            addExpected(expected);
            throw getSyntaxError();
        }
        read();
    }

    private void read(int tokenType) {
        if (tokenType != currentTokenType) {
            addExpected(tokenType);
            throw getSyntaxError();
        }
        read();
    }

    private boolean readIf(String token) {
        if (!currentTokenQuoted && equalsToken(token, currentToken)) {
            read();
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean readIf(int tokenType) {
        if (tokenType == currentTokenType) {
            read();
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    private boolean isToken(String token) {
        if (!currentTokenQuoted && equalsToken(token, currentToken)) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean isToken(int tokenType) {
        if (tokenType == currentTokenType) {
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    private boolean equalsToken(String a, String b) {
        if (a == null) {
            return b == null;
        } else
            return a.equals(b) || !identifiersToUpper && a.equalsIgnoreCase(b);
    }

    private boolean isIdentifier() {
        return currentTokenType == IDENTIFIER || nonKeywords != null && nonKeywords.get(currentTokenType);
    }

    private void addExpected(String token) {
        if (expectedList != null) {
            expectedList.add(token);
        }
    }

    private void addExpected(int tokenType) {
        if (expectedList != null) {
            expectedList.add(TOKENS[tokenType]);
        }
    }

    private void addMultipleExpected(int ... tokenTypes) {
        for (int tokenType : tokenTypes) {
            expectedList.add(TOKENS[tokenType]);
        }
    }

    private void reread(int index) {
        if (lastParseIndex != index) {
            parseIndex = index;
            read();
        }
    }

    private void read() {
        currentTokenQuoted = false;
        if (expectedList != null) {
            expectedList.clear();
        }
        int[] types = characterTypes;
        lastParseIndex = parseIndex;
        int i = parseIndex;
        int type;
        while ((type = types[i]) == 0) {
            i++;
        }
        int start = i;
        char[] chars = sqlCommandChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
        case CHAR_NAME:
            switch (c) {
            case 'N':
            case 'n':
                if (chars[i] == '\'') {
                    readString(i + 1, chars, types);
                    return;
                }
                break;
            case 'X':
            case 'x':
                if (chars[i] == '\'') {
                    ByteArrayOutputStream result = new ByteArrayOutputStream();
                    for (;;) {
                        int begin = ++i;
                        while (chars[i] != '\'') {
                            i++;
                        }
                        StringUtils.convertHexWithSpacesToBytes(result, sqlCommandChars, begin, i);
                        begin = ++i;
                        while ((type = types[i]) == 0) {
                            i++;
                        }
                        if (begin == i || type != CHAR_STRING) {
                            break;
                        }
                    }
                    currentToken = "X'";
                    checkLiterals(true);
                    currentValue = ValueVarbinary.get(result.toByteArray());
                    parseIndex = i;
                    currentTokenType = LITERAL;
                    return;
                }
                break;
            case 'U':
            case 'u':
                if (chars[i] == '&') {
                    switch (chars[i + 1]) {
                    case '\'': {
                        String s = readRawString(i + 2, chars, types);
                        currentValue = ValueVarchar.get(StringUtils.decodeUnicodeStringSQL(s,
                                readUescape(parseIndex, chars, types)));
                        return;
                    }
                    case '"': {
                        readQuotedIdentifier(i + 2, '"', chars, false);
                        String identifier = currentToken;
                        i = parseIndex;
                        while (types[i] == 0) {
                            i++;
                        }
                        identifier = StringUtils.decodeUnicodeStringSQL(identifier, readUescape(i, chars, types));
                        if (identifier.length() > Constants.MAX_IDENTIFIER_LENGTH) {
                            throw DbException.get(ErrorCode.NAME_TOO_LONG_2, identifier.substring(0, 32),
                                    "" + Constants.MAX_IDENTIFIER_LENGTH);
                        }
                        currentToken = StringUtils.cache(identifier);
                        currentTokenQuoted = true;
                        currentTokenType = IDENTIFIER;
                        return;
                    }
                    }
                }
            }
            while ((type = types[i]) == CHAR_NAME || type == CHAR_VALUE) {
                i++;
            }
            currentTokenType = ParserUtil.getTokenType(sqlCommand, !identifiersToUpper, start, i - start, false);
            if (isIdentifier()) {
                currentToken = StringUtils.cache(checkIdentifierLength(start, i));
            } else {
                currentToken = TOKENS[currentTokenType];
            }
            parseIndex = i;
            return;
        case CHAR_QUOTED:
            readQuotedIdentifier(i, c, chars, true);
            return;
        case CHAR_SPECIAL_2:
            if (types[i] == CHAR_SPECIAL_2) {
                char c1 = chars[i++];
                currentTokenType = getSpecialType2(c, c1);
            } else {
                currentTokenType = getSpecialType1(c);
            }
            parseIndex = i;
            return;
        case CHAR_SPECIAL_1:
            currentTokenType = getSpecialType1(c);
            parseIndex = i;
            return;
        case CHAR_VALUE:
            if (c == '0' && (chars[i] == 'X' || chars[i] == 'x')) {
                readHexNumber(i + 1, start + 2, chars, types);
                return;
            }
            long number = c - '0';
            loop: for (;; i++) {
                c = chars[i];
                if (c < '0' || c > '9') {
                    switch (c) {
                    case '.':
                        readNumeric(start, i, false, false);
                        break loop;
                    case 'E':
                    case 'e':
                        readNumeric(start, i, false, true);
                        break loop;
                    case 'L':
                    case 'l':
                        readNumeric(start, i, true, false);
                        break loop;
                    }
                    checkLiterals(false);
                    currentValue = ValueInteger.get((int) number);
                    currentTokenType = LITERAL;
                    currentToken = "0";
                    parseIndex = i;
                    break;
                }
                number = number * 10 + (c - '0');
                if (number > Integer.MAX_VALUE) {
                    readNumeric(start, i, true, false);
                    break;
                }
            }
            return;
        case CHAR_DOT:
            if (types[i] != CHAR_VALUE) {
                currentTokenType = DOT;
                currentToken = ".";
                parseIndex = i;
                return;
            }
            readNumeric(i - 1, i, false, false);
            return;
        case CHAR_STRING:
            readString(i, chars, types);
            return;
        case CHAR_DOLLAR_QUOTED_STRING: {
            int begin = i - 1;
            while (types[i] == CHAR_DOLLAR_QUOTED_STRING) {
                i++;
            }
            String result = sqlCommand.substring(begin, i);
            currentToken = "'";
            checkLiterals(true);
            currentValue = ValueVarchar.get(result, database);
            parseIndex = i;
            currentTokenType = LITERAL;
            return;
        }
        case CHAR_END:
            currentTokenType = END_OF_INPUT;
            parseIndex = i;
            return;
        default:
            throw getSyntaxError();
        }
    }

    private void readQuotedIdentifier(int i, char c, char[] chars, boolean checkLength) {
        int begin = i;
        while (chars[i] != c) {
            i++;
        }
        String result = checkLength ? checkIdentifierLength(begin, i) : sqlCommand.substring(begin, i);
        if (chars[++i] == c) {
            StringBuilder builder = new StringBuilder(result);
            do {
                begin = i;
                while (chars[++i] != c) {}
                if (checkLength) {
                    checkIdentifierLength(builder, begin, i);
                }
            } while (chars[++i] == c);
            result = builder.toString();
        }
        currentToken = StringUtils.cache(result);
        parseIndex = i;
        currentTokenQuoted = true;
        currentTokenType = IDENTIFIER;
    }

    private String checkIdentifierLength(int begin, int end) {
        if (end - begin > Constants.MAX_IDENTIFIER_LENGTH) {
            throw DbException.get(ErrorCode.NAME_TOO_LONG_2, sqlCommand.substring(begin, begin + 32),
                    "" + Constants.MAX_IDENTIFIER_LENGTH);
        }
        return sqlCommand.substring(begin, end);
    }

    private void checkIdentifierLength(StringBuilder builder, int begin, int end) {
        int length = builder.length();
        if (length + end - begin > Constants.MAX_IDENTIFIER_LENGTH) {
            if (length < 32) {
                builder.append(sqlCommand, begin, begin + 32 - length);
            } else {
                builder.setLength(32);
            }
            throw DbException.get(ErrorCode.NAME_TOO_LONG_2, builder.toString(), "" + Constants.MAX_IDENTIFIER_LENGTH);
        }
        builder.append(sqlCommand, begin, end);
    }

    private void readParameterIndex() {
        int i = parseIndex;
        char[] chars = sqlCommandChars;
        char c = chars[i++];
        long number = c - '0';
        for (; (c = chars[i]) >= '0' && c <= '9'; i++) {
            number = number * 10 + (c - '0');
            if (number > Integer.MAX_VALUE) {
                throw DbException.getInvalidValueException(
                        "parameter index", number);
            }
        }
        currentValue = ValueInteger.get((int) number);
        currentTokenType = LITERAL;
        currentToken = "0";
        parseIndex = i;
    }

    private void checkLiterals(boolean text) {
        if (!literalsChecked && session != null && !session.getAllowLiterals()) {
            int allowed = database.getAllowLiterals();
            if (allowed == Constants.ALLOW_LITERALS_NONE ||
                    (text && allowed != Constants.ALLOW_LITERALS_ALL)) {
                throw DbException.get(ErrorCode.LITERALS_ARE_NOT_ALLOWED);
            }
        }
    }

    private void readString(int i, char[] chars, int[] types) {
        currentValue = ValueVarchar.get(readRawString(i, chars, types), database);
    }

    private String readRawString(int i, char[] chars, int[] types) {
        String result = null;
        StringBuilder builder = null;
        for (;; i++) {
            boolean next = false;
            for (;; i++) {
                int begin = i;
                while (chars[i] != '\'') {
                    i++;
                }
                if (result == null) {
                    result = sqlCommand.substring(begin, i);
                } else {
                    if (builder == null) {
                        builder = new StringBuilder(result);
                    }
                    builder.append(sqlCommand, next ? begin - 1 : begin, i);
                }
                if (chars[++i] != '\'') {
                    break;
                }
                next = true;
            }
            int type;
            while ((type = types[i]) == 0) {
                i++;
            }
            if (type != CHAR_STRING) {
                break;
            }
        }
        checkLiterals(true);
        parseIndex = i;
        currentToken = "'";
        currentTokenType = LITERAL;
        return builder != null ? builder.toString() : result;
    }

    private int readUescape(int i, char[] chars, int[] types) {
        int start = i;
        while (types[i] == CHAR_NAME) {
            i++;
        }
        if (i - start == 7 && "UESCAPE".regionMatches(!identifiersToUpper, 0, sqlCommand, start, 7)) {
            int type;
            while ((type = types[i]) == 0) {
                i++;
            }
            if (type == CHAR_STRING) {
                String s = readRawString(i + 1, chars, types);
                if (s.codePointCount(0, s.length()) == 1) {
                    int escape = s.codePointAt(0);
                    if (!Character.isWhitespace(escape) && (escape < '0' || escape > '9')
                            && (escape < 'A' || escape > 'F') && (escape < 'a' || escape > 'f')) {
                        switch (escape) {
                        default:
                            return escape;
                        case '"':
                        case '\'':
                        case '+':
                        }
                    }
                }
            }
            addExpected("'<Unicode escape character>'");
            throw getSyntaxError();
        }
        return '\\';
    }

    private void readHexNumber(int i, int start, char[] chars, int[] types) {
        if (database.getMode().zeroExLiteralsAreBinaryStrings) {
            for (char c; (c = chars[i]) >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'z';) {
                i++;
            }
            if (types[i] == CHAR_NAME) {
                throw DbException.get(ErrorCode.HEX_STRING_WRONG_1, sqlCommand.substring(i, i + 1));
            }
            checkLiterals(true);
            currentValue = ValueVarbinary.getNoCopy(StringUtils.convertHexToBytes(sqlCommand.substring(start, i)));
            parseIndex = i;
        } else {
            long number = 0;
            for (;; i++) {
                char c = chars[i];
                if (c >= '0' && c <= '9') {
                    number = (number << 4) + c - '0';
                } else if ((c &= 0xffdf) >= 'A' && c <= 'F') { // Convert a-z to A-Z
                    number = (number << 4) + c - ('A' - 10);
                } else if (i == start) {
                    parseIndex = i;
                    addExpected("Hex number");
                    throw getSyntaxError();
                } else {
                    currentValue = ValueInteger.get((int) number);
                    break;
                }
                if (number > Integer.MAX_VALUE) {
                    do {
                        c = chars[++i];
                    } while ((c >= '0' && c <= '9') || ((c &= 0xffdf) >= 'A' && c <= 'F')); // Convert a-z to A-Z
                    String sub = sqlCommand.substring(start, i);
                    currentValue = ValueNumeric.get(new BigInteger(sub, 16));
                    break;
                }
            }
            char c = chars[i];
            if (c == 'L' || c == 'l') {
                i++;
            }
            parseIndex = i;
            if (types[i] == CHAR_NAME) {
                addExpected("Hex number");
                throw getSyntaxError();
            }
            checkLiterals(false);
        }
        currentTokenType = LITERAL;
        currentToken = "0";
    }

    private void readNumeric(int start, int i, boolean integer, boolean approximate) {
        char[] chars = sqlCommandChars;
        int[] types = characterTypes;
        // go until the first non-number
        for (;; i++) {
            int t = types[i];
            if (t == CHAR_DOT) {
                integer = false;
            } else if (t != CHAR_VALUE) {
                break;
            }
        }
        char c = chars[i];
        if (c == 'E' || c == 'e') {
            integer = false;
            approximate = true;
            c = chars[++i];
            if (c == '+' || c == '-') {
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
        checkLiterals(false);
        if (integer && i - start <= 19) {
            BigInteger bi = new BigInteger(sqlCommand.substring(start, i));
            if (bi.compareTo(ValueBigint.MAX_BI) <= 0) {
                // parse constants like "10000000L"
                c = chars[i];
                if (c == 'L' || c == 'l') {
                    parseIndex++;
                }
                currentValue = ValueBigint.get(bi.longValue());
                currentTokenType = LITERAL;
                return;
            }
            currentValue = ValueNumeric.get(bi);
        } else {
            BigDecimal bd;
            try {
                bd = new BigDecimal(sqlCommandChars, start, i - start);
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, sqlCommand.substring(start, i));
            }
            currentValue = approximate ? ValueDecfloat.get(bd) : ValueNumeric.get(bd);
        }
        currentTokenType = LITERAL;
    }

    private void initialize(String sql) {
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
                    for (int level = 1; level > 0;) {
                        for (;;) {
                            checkRunOver(i, len, startLoop);
                            char ch = command[i];
                            command[i++] = ' ';
                            if (ch == '*') {
                                if (command[i] == '/') {
                                    level--;
                                    break;
                                }
                            } else if (ch == '/' && command[i] == '*') {
                                level++;
                                command[i++] = ' ';
                            }
                        }
                        command[i] = ' ';
                    }
                } else if (command[i + 1] == '/') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while ((c = command[i]) != '\n' && c != '\r' && i < len - 1) {
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
                    while ((c = command[i]) != '\n' && c != '\r' && i < len - 1) {
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '$':
                if (command[i + 1] == '$' && (i == 0 || command[i - 1] <= ' ')) {
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
            case '&':
            case '~':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DOT;
                break;
            case '\'':
                type = types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '?':
                type = CHAR_SPECIAL_1;
                if (command[i + 1] == '?') {
                    char ch = command[i + 2];
                    if (ch == '(') {
                        command[i + 1] = command[i] = ' ';
                        command[i += 2] = '[';
                        changed = true;
                    } else if (ch == ')') {
                        command[i + 1] = command[i] = ' ';
                        command[i += 2] = ']';
                        changed = true;
                    }
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
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '`') {
                    checkRunOver(i, len, startLoop);
                    c = command[i];
                    if (identifiersToUpper || identifiersToLower) {
                        char u = identifiersToUpper ? Character.toUpperCase(c) : Character.toLowerCase(c);
                        if (u != c) {
                            command[i] = u;
                            changed = true;
                        }
                    }
                }
                break;
            case '"':
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '_':
                type = CHAR_NAME;
                break;
            case '#':
                if (database.getMode().supportPoundSymbolForColumnNames) {
                    type = CHAR_NAME;
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            default:
                if (c >= 'a' && c <= 'z') {
                    if (identifiersToUpper) {
                        command[i] = (char) (c - ('a' - 'A'));
                        changed = true;
                    }
                    type = CHAR_NAME;
                } else if (c >= 'A' && c <= 'Z') {
                    if (identifiersToLower) {
                        command[i] = (char) (c + ('a' - 'A'));
                        changed = true;
                    }
                    type = CHAR_NAME;
                } else if (c >= '0' && c <= '9') {
                    type = CHAR_VALUE;
                } else {
                    if (c <= ' ' || Character.isSpaceChar(c)) {
                        // whitespace
                    } else if (Character.isJavaIdentifierPart(c)) {
                        type = CHAR_NAME;
                        if (identifiersToUpper || identifiersToLower) {
                            char u = identifiersToUpper ? Character.toUpperCase(c) : Character.toLowerCase(c);
                            if (u != c) {
                                command[i] = u;
                                changed = true;
                            }
                        }
                    } else {
                        type = CHAR_SPECIAL_1;
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
            sqlCommand = new String(command, 0, len);
        }
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) {
        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private int getSpecialType1(char c0) {
        switch (c0) {
        case '?':
        case '$':
            return PARAMETER;
        case '@':
            return AT;
        case '+':
            return PLUS_SIGN;
        case '-':
            return MINUS_SIGN;
        case '*':
            return ASTERISK;
        case ',':
            return COMMA;
        case '{':
            return OPEN_BRACE;
        case '}':
            return CLOSE_BRACE;
        case '/':
            return SLASH;
        case '%':
            return PERCENT;
        case '&':
            return AMPERSAND;
        case ';':
            return SEMICOLON;
        case ':':
            return COLON;
        case '[':
            return OPEN_BRACKET;
        case ']':
            return CLOSE_BRACKET;
        case '~':
            return TILDE;
        case '(':
            return OPEN_PAREN;
        case ')':
            return CLOSE_PAREN;
        case '<':
            return SMALLER;
        case '>':
            return BIGGER;
        case '=':
            return EQUAL;
        default:
            throw getSyntaxError();
        }
    }

    private int getSpecialType2(char c0, char c1) {
        switch (c0) {
        case ':':
            if (c1 == ':') {
                return COLON_COLON;
            } else if (c1 == '=') {
                return COLON_EQ;
            }
            break;
        case '>':
            if (c1 == '=') {
                return BIGGER_EQUAL;
            }
            break;
        case '<':
            if (c1 == '=') {
                return SMALLER_EQUAL;
            } else if (c1 == '>') {
                return NOT_EQUAL;
            }
            break;
        case '!':
            if (c1 == '=') {
                return NOT_EQUAL;
            } else if (c1 == '~') {
                return NOT_TILDE;
            }
            break;
        case '|':
            if (c1 == '|') {
                return CONCATENATION;
            }
            break;
        case '&':
            if (c1 == '&') {
                return SPATIAL_INTERSECTS;
            }
            break;
        }
        throw getSyntaxError();
    }

    private static boolean isKeyword(int tokenType) {
        return tokenType >= FIRST_KEYWORD && tokenType <= LAST_KEYWORD;
    }

    private boolean isKeyword(String s) {
        return ParserUtil.isKeyword(s, !identifiersToUpper);
    }

    private String upperName(String name) {
        return identifiersToUpper ? name : StringUtils.toUpperEnglish(name);
    }

    private Column parseColumnForTable(String columnName, boolean defaultNullable) {
        Column column;
        Mode mode = database.getMode();
        if (mode.identityDataType && readIf("IDENTITY")) {
            column = new Column(columnName, TypeInfo.TYPE_BIGINT);
            parseCompatibilityIdentityOptions(column);
            column.setPrimaryKey(true);
        } else if (mode.serialDataTypes && readIf("BIGSERIAL")) {
            column = new Column(columnName, TypeInfo.TYPE_BIGINT);
            column.setIdentityOptions(new SequenceOptions(), false);
        } else if (mode.serialDataTypes && readIf("SERIAL")) {
            column = new Column(columnName, TypeInfo.TYPE_INTEGER);
            column.setIdentityOptions(new SequenceOptions(), false);
        } else {
            column = parseColumnWithType(columnName);
        }
        if (readIf("INVISIBLE")) {
            column.setVisible(false);
        } else if (readIf("VISIBLE")) {
            column.setVisible(true);
        }
        boolean defaultOnNull = false;
        NullConstraintType nullConstraint = parseNotNullConstraint();
        defaultIdentityGeneration: if (!column.isIdentity()) {
            if (readIf(AS)) {
                column.setGeneratedExpression(readExpression());
            } else if (readIf(DEFAULT)) {
                if (readIf(ON)) {
                    read(NULL);
                    defaultOnNull = true;
                    break defaultIdentityGeneration;
                }
                column.setDefaultExpression(session, readExpression());
            } else if (readIf("GENERATED")) {
                boolean always = readIf("ALWAYS");
                if (!always) {
                    read("BY");
                    read(DEFAULT);
                }
                read(AS);
                if (readIf("IDENTITY")) {
                    SequenceOptions options = new SequenceOptions();
                    if (readIf(OPEN_PAREN)) {
                        parseSequenceOptions(options, null, false, false);
                        read(CLOSE_PAREN);
                    }
                    column.setIdentityOptions(options, always);
                    break defaultIdentityGeneration;
                } else if (!always) {
                    throw getSyntaxError();
                } else {
                    column.setGeneratedExpression(readExpression());
                }
            }
            if (!column.isGenerated() && readIf(ON)) {
                read("UPDATE");
                column.setOnUpdateExpression(session, readExpression());
            }
            nullConstraint = parseNotNullConstraint(nullConstraint);
            if (parseCompatibilityIdentity(column, mode)) {
                nullConstraint = parseNotNullConstraint(nullConstraint);
            }
        }
        switch (nullConstraint) {
        case NULL_IS_ALLOWED:
            if (column.isIdentity()) {
                throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
            }
            column.setNullable(true);
            break;
        case NULL_IS_NOT_ALLOWED:
            column.setNullable(false);
            break;
        case NO_NULL_CONSTRAINT_FOUND:
            if (!column.isIdentity()) {
                column.setNullable(defaultNullable);
            }
            break;
        default:
            throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                    "Internal Error - unhandled case: " + nullConstraint.name());
        }
        if (!defaultOnNull) {
            if (readIf(DEFAULT)) {
                read(ON);
                read(NULL);
                defaultOnNull = true;
            } else if (readIf("NULL_TO_DEFAULT")) {
                defaultOnNull = true;
            }
        }
        if (defaultOnNull) {
            column.setDefaultOnNull(true);
        }
        if (!column.isGenerated()) {
            if (readIf("SEQUENCE")) {
                column.setSequence(readSequence(), column.isGeneratedAlways());
            }
        }
        if (readIf("SELECTIVITY")) {
            column.setSelectivity(readNonNegativeInt());
        }
        if (mode.getEnum() == ModeEnum.MySQL) {
            if (readIf("CHARACTER")) {
                readIf(SET);
                readMySQLCharset();
            }
            if (readIf("COLLATE")) {
                readMySQLCharset();
            }
        }
        String comment = readCommentIf();
        if (comment != null) {
            column.setComment(comment);
        }
        return column;
    }

    private void parseCompatibilityIdentityOptions(Column column) {
        SequenceOptions options = new SequenceOptions();
        if (readIf(OPEN_PAREN)) {
            options.setStartValue(ValueExpression.get(ValueBigint.get(readLong())));
            if (readIf(COMMA)) {
                options.setIncrement(ValueExpression.get(ValueBigint.get(readLong())));
            }
            read(CLOSE_PAREN);
        }
        column.setIdentityOptions(options, false);
    }

    private String readCommentIf() {
        if (readIf("COMMENT")) {
            readIf(IS);
            return readString();
        }
        return null;
    }

    private Column parseColumnWithType(String columnName) {
        TypeInfo typeInfo = readIfDataType();
        if (typeInfo == null) {
            String domainName = readIdentifierWithSchema();
            return getColumnWithDomain(columnName, getSchema().getDomain(domainName));
        }
        return new Column(columnName, typeInfo);
    }

    private TypeInfo parseDataType() {
        TypeInfo typeInfo = readIfDataType();
        if (typeInfo == null) {
            addExpected("data type");
            throw getSyntaxError();
        }
        return typeInfo;
    }

    private TypeInfo readIfDataType() {
        TypeInfo typeInfo = readIfDataType1();
        if (typeInfo != null) {
            while (readIf(ARRAY)) {
                typeInfo = parseArrayType(typeInfo);
            }
        }
        return typeInfo;
    }

    private TypeInfo readIfDataType1() {
        switch (currentTokenType) {
        case IDENTIFIER:
            if (currentTokenQuoted) {
                return null;
            }
            break;
        case INTERVAL: {
            read();
            TypeInfo typeInfo = readIntervalQualifier();
            if (typeInfo == null) {
                throw intervalQualifierError();
            }
            return typeInfo;
        }
        case NULL:
            read();
            return TypeInfo.TYPE_NULL;
        case ROW:
            read();
            return parseRowType();
        case ARRAY:
            // Partial compatibility with 1.4.200 and older versions
            if (session.isQuirksMode()) {
                read();
                return parseArrayType(TypeInfo.TYPE_VARCHAR);
            }
            addExpected("data type");
            throw getSyntaxError();
        default:
            if (isKeyword(currentToken)) {
                break;
            }
            addExpected("data type");
            throw getSyntaxError();
        }
        int index = lastParseIndex;
        String originalCase = currentToken;
        read();
        if (currentTokenType == DOT) {
            reread(index);
            return null;
        }
        String original = upperName(originalCase);
        switch (original) {
        case "BINARY":
            if (readIf("VARYING")) {
                original = "BINARY VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "BINARY LARGE OBJECT";
            } else if (variableBinary) {
                original = "VARBINARY";
            }
            break;
        case "CHAR":
            if (readIf("VARYING")) {
                original = "CHAR VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "CHAR LARGE OBJECT";
            }
            break;
        case "CHARACTER":
            if (readIf("VARYING")) {
                original = "CHARACTER VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "CHARACTER LARGE OBJECT";
            }
            break;
        case "DATETIME":
        case "DATETIME2":
            return parseDateTimeType(false);
        case "DEC":
        case "DECIMAL":
            return parseNumericType(true);
        case "DECFLOAT":
            return parseDecfloatType();
        case "DOUBLE":
            if (readIf("PRECISION")) {
                original = "DOUBLE PRECISION";
            }
            break;
        case "ENUM":
            return parseEnumType();
        case "FLOAT":
            return parseFloatType();
        case "GEOMETRY":
            return parseGeometryType();
        case "LONG":
            if (readIf("RAW")) {
                original = "LONG RAW";
            }
            break;
        case "NATIONAL":
            if (readIf("CHARACTER")) {
                if (readIf("VARYING")) {
                    original = "NATIONAL CHARACTER VARYING";
                } else if (readIf("LARGE")) {
                    read("OBJECT");
                    original = "NATIONAL CHARACTER LARGE OBJECT";
                } else {
                    original = "NATIONAL CHARACTER";
                }
            } else {
                read("CHAR");
                if (readIf("VARYING")) {
                    original = "NATIONAL CHAR VARYING";
                } else {
                    original = "NATIONAL CHAR";
                }
            }
            break;
        case "NCHAR":
            if (readIf("VARYING")) {
                original = "NCHAR VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "NCHAR LARGE OBJECT";
            }
            break;
        case "NUMBER":
            if (database.getMode().disallowedTypes.contains("NUMBER")) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NUMBER");
            }
            if (!isToken(OPEN_PAREN)) {
                return TypeInfo.getTypeInfo(Value.DECFLOAT, 40, -1, null);
            }
            //$FALL-THROUGH$
        case "NUMERIC":
            return parseNumericType(false);
        case "SMALLDATETIME":
            return parseDateTimeType(true);
        case "TIME":
            return parseTimeType();
        case "TIMESTAMP":
            return parseTimestampType();
        }
        // Domain names can't have multiple words without quotes
        if (originalCase.length() == original.length()) {
            Domain domain = database.getSchema(session.getCurrentSchemaName()).findDomain(originalCase);
            if (domain != null) {
                reread(index);
                return null;
            }
        }
        Mode mode = database.getMode();
        DataType dataType = DataType.getTypeByName(original, mode);
        if (dataType == null || mode.disallowedTypes.contains(original)) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, original);
        }
        long precision;
        int scale;
        if (dataType.specialPrecisionScale) {
            precision = dataType.defaultPrecision;
            scale = dataType.defaultScale;
        } else {
            precision = -1L;
            scale = -1;
        }
        int t = dataType.type;
        if (database.getIgnoreCase() && t == Value.VARCHAR && !equalsToken("VARCHAR_CASESENSITIVE", original)) {
            dataType = DataType.getDataType(t = Value.VARCHAR_IGNORECASE);
        }
        if ((dataType.supportsPrecision || dataType.supportsScale) && readIf(OPEN_PAREN)) {
            if (!readIf("MAX")) {
                if (dataType.supportsPrecision) {
                    precision = readPrecision(t);
                    if (precision < dataType.minPrecision) {
                        throw getInvalidPrecisionException(dataType, precision);
                    } else if (precision > dataType.maxPrecision)
                    badPrecision: {
                        if (session.isQuirksMode() || session.isTruncateLargeLength()) {
                            switch (dataType.type) {
                            case Value.CHAR:
                            case Value.VARCHAR:
                            case Value.VARCHAR_IGNORECASE:
                            case Value.BINARY:
                            case Value.VARBINARY:
                            case Value.JAVA_OBJECT:
                            case Value.JSON:
                                precision = dataType.maxPrecision;
                                break badPrecision;
                            }
                        }
                        throw getInvalidPrecisionException(dataType, precision);
                    }
                    if (dataType.supportsScale) {
                        if (readIf(COMMA)) {
                            scale = readInt();
                            if (scale < dataType.minScale || scale > dataType.maxScale) {
                                throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale),
                                        Integer.toString(dataType.minScale), Integer.toString(dataType.maxScale));
                            }
                        }
                    }
                } else {
                    scale = readInt();
                    if (scale < dataType.minScale || scale > dataType.maxScale) {
                        throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale),
                                Integer.toString(dataType.minScale), Integer.toString(dataType.maxScale));
                    }
                }
            }
            read(CLOSE_PAREN);
        }
        if (mode.allNumericTypesHavePrecision && DataType.isNumericType(dataType.type)) {
            if (readIf(OPEN_PAREN)) {
                // Support for MySQL: INT(11), MEDIUMINT(8) and so on.
                // Just ignore the precision.
                readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            readIf("UNSIGNED");
        }
        if (mode.forBitData && DataType.isStringType(t)) {
            if (readIf(FOR)) {
                read("BIT");
                read("DATA");
                dataType = DataType.getDataType(t = Value.VARBINARY);
            }
        }
        return TypeInfo.getTypeInfo(t, precision, scale, null);
    }

    private static DbException getInvalidPrecisionException(DataType dataType, long precision) {
        return DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Long.toString(precision),
                Long.toString(dataType.minPrecision), Long.toString(dataType.maxPrecision));
    }

    private static Column getColumnWithDomain(String columnName, Domain domain) {
        Column column = new Column(columnName, domain.getDataType());
        column.setComment(domain.getComment());
        column.setDomain(domain);
        return column;
    }

    private TypeInfo parseFloatType() {
        int type = Value.DOUBLE;
        int precision;
        if (readIf(OPEN_PAREN)) {
            precision = readNonNegativeInt();
            read(CLOSE_PAREN);
            if (precision < 1 || precision > 53) {
                throw DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Integer.toString(precision), "1", "53");
            }
            if (precision <= 24) {
                type = Value.REAL;
            }
        } else {
            precision = 0;
        }
        return TypeInfo.getTypeInfo(type, precision, -1, null);
    }

    private TypeInfo parseNumericType(boolean decimal) {
        long precision = -1L;
        int scale = -1;
        if (readIf(OPEN_PAREN)) {
            precision = readPrecision(Value.NUMERIC);
            if (precision < 1) {
                throw getInvalidNumericPrecisionException(precision);
            } else if (precision > Constants.MAX_NUMERIC_PRECISION) {
                if (session.isQuirksMode() || session.isTruncateLargeLength()) {
                    precision = Constants.MAX_NUMERIC_PRECISION;
                } else {
                    throw getInvalidNumericPrecisionException(precision);
                }
            }
            if (readIf(COMMA)) {
                scale = readInt();
                if (scale < 0 || scale > ValueNumeric.MAXIMUM_SCALE) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale),
                            "0", "" + ValueNumeric.MAXIMUM_SCALE);
                }
            }
            read(CLOSE_PAREN);
        }
        return TypeInfo.getTypeInfo(Value.NUMERIC, precision, scale, decimal ? ExtTypeInfoNumeric.DECIMAL : null);
    }

    private TypeInfo parseDecfloatType() {
        long precision = -1L;
        if (readIf(OPEN_PAREN)) {
            precision = readPrecision(Value.DECFLOAT);
            if (precision < 1 || precision > Constants.MAX_NUMERIC_PRECISION) {
                throw getInvalidNumericPrecisionException(precision);
            }
            read(CLOSE_PAREN);
        }
        return TypeInfo.getTypeInfo(Value.DECFLOAT, precision, -1, null);
    }

    private static DbException getInvalidNumericPrecisionException(long precision) {
        return DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Long.toString(precision), "1",
                "" + Constants.MAX_NUMERIC_PRECISION);
    }

    private TypeInfo parseTimeType() {
        int scale = -1;
        if (readIf(OPEN_PAREN)) {
            scale = readNonNegativeInt();
            if (scale > ValueTime.MAXIMUM_SCALE) {
                throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale), "0",
                        /* Folds to a constant */ "" + ValueTime.MAXIMUM_SCALE);
            }
            read(CLOSE_PAREN);
        }
        int type = Value.TIME;
        if (readIf(WITH)) {
            read("TIME");
            read("ZONE");
            type = Value.TIME_TZ;
        } else if (readIf("WITHOUT")) {
            read("TIME");
            read("ZONE");
        }
        return TypeInfo.getTypeInfo(type, -1L, scale, null);
    }

    private TypeInfo parseTimestampType() {
        int scale = -1;
        if (readIf(OPEN_PAREN)) {
            scale = readNonNegativeInt();
            // Allow non-standard TIMESTAMP(..., ...) syntax
            if (readIf(COMMA)) {
                scale = readNonNegativeInt();
            }
            if (scale > ValueTimestamp.MAXIMUM_SCALE) {
                throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale), "0",
                        /* Folds to a constant */ "" + ValueTimestamp.MAXIMUM_SCALE);
            }
            read(CLOSE_PAREN);
        }
        int type = Value.TIMESTAMP;
        if (readIf(WITH)) {
            read("TIME");
            read("ZONE");
            type = Value.TIMESTAMP_TZ;
        } else if (readIf("WITHOUT")) {
            read("TIME");
            read("ZONE");
        }
        return TypeInfo.getTypeInfo(type, -1L, scale, null);
    }

    private TypeInfo parseDateTimeType(boolean smallDateTime) {
        int scale;
        if (smallDateTime) {
            scale = 0;
        } else {
            scale = -1;
            if (readIf(OPEN_PAREN)) {
                scale = readNonNegativeInt();
                if (scale > ValueTimestamp.MAXIMUM_SCALE) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale), "0",
                            /* folds to a constant */ "" + ValueTimestamp.MAXIMUM_SCALE);
                }
                read(CLOSE_PAREN);
            }
        }
        return TypeInfo.getTypeInfo(Value.TIMESTAMP, -1L, scale, null);
    }

    private TypeInfo readIntervalQualifier() {
        IntervalQualifier qualifier;
        int precision = -1, scale = -1;
        switch (currentTokenType) {
        case YEAR:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            if (readIf(TO)) {
                read(MONTH);
                qualifier = IntervalQualifier.YEAR_TO_MONTH;
            } else {
                qualifier = IntervalQualifier.YEAR;
            }
            break;
        case MONTH:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            qualifier = IntervalQualifier.MONTH;
            break;
        case DAY:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            if (readIf(TO)) {
                switch (currentTokenType) {
                case HOUR:
                    read();
                    qualifier = IntervalQualifier.DAY_TO_HOUR;
                    break;
                case MINUTE:
                    read();
                    qualifier = IntervalQualifier.DAY_TO_MINUTE;
                    break;
                case SECOND:
                    read();
                    if (readIf(OPEN_PAREN)) {
                        scale = readNonNegativeInt();
                        read(CLOSE_PAREN);
                    }
                    qualifier = IntervalQualifier.DAY_TO_SECOND;
                    break;
                default:
                    throw intervalDayError();
                }
            } else {
                qualifier = IntervalQualifier.DAY;
            }
            break;
        case HOUR:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            if (readIf(TO)) {
                switch (currentTokenType) {
                case MINUTE:
                    read();
                    qualifier = IntervalQualifier.HOUR_TO_MINUTE;
                    break;
                case SECOND:
                    read();
                    if (readIf(OPEN_PAREN)) {
                        scale = readNonNegativeInt();
                        read(CLOSE_PAREN);
                    }
                    qualifier = IntervalQualifier.HOUR_TO_SECOND;
                    break;
                default:
                    throw intervalHourError();
                }
            } else {
                qualifier = IntervalQualifier.HOUR;
            }
            break;
        case MINUTE:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                read(CLOSE_PAREN);
            }
            if (readIf(TO)) {
                read(SECOND);
                if (readIf(OPEN_PAREN)) {
                    scale = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                qualifier = IntervalQualifier.MINUTE_TO_SECOND;
            } else {
                qualifier = IntervalQualifier.MINUTE;
            }
            break;
        case SECOND:
            read();
            if (readIf(OPEN_PAREN)) {
                precision = readNonNegativeInt();
                if (readIf(COMMA)) {
                    scale = readNonNegativeInt();
                }
                read(CLOSE_PAREN);
            }
            qualifier = IntervalQualifier.SECOND;
            break;
        default:
            return null;
        }
        if (precision >= 0) {
            if (precision == 0 || precision > ValueInterval.MAXIMUM_PRECISION) {
                throw DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Integer.toString(precision), "1",
                        /* Folds to a constant */ "" + ValueInterval.MAXIMUM_PRECISION);
            }
        }
        if (scale >= 0) {
            if (scale > ValueInterval.MAXIMUM_SCALE) {
                throw DbException.get(ErrorCode.INVALID_VALUE_SCALE, Integer.toString(scale), "0",
                        /* Folds to a constant */ "" + ValueInterval.MAXIMUM_SCALE);
            }
        }
        return TypeInfo.getTypeInfo(qualifier.ordinal() + Value.INTERVAL_YEAR, precision, scale, null);
    }

    private DbException intervalQualifierError() {
        if (expectedList != null) {
            addMultipleExpected(YEAR, MONTH, DAY, HOUR, MINUTE, SECOND);
        }
        return getSyntaxError();
    }

    private DbException intervalDayError() {
        if (expectedList != null) {
            addMultipleExpected(HOUR, MINUTE, SECOND);
        }
        return getSyntaxError();
    }

    private DbException intervalHourError() {
        if (expectedList != null) {
            addMultipleExpected(MINUTE, SECOND);
        }
        return getSyntaxError();
    }

    private TypeInfo parseArrayType(TypeInfo componentType) {
        int precision = -1;
        if (readIf(OPEN_BRACKET)) {
            // Maximum cardinality may be zero
            precision = readNonNegativeInt();
            if (precision > Constants.MAX_ARRAY_CARDINALITY) {
                throw DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Integer.toString(precision), "0",
                        /* Folds to a constant */ "" + Constants.MAX_ARRAY_CARDINALITY);
            }
            read(CLOSE_BRACKET);
        }
        return TypeInfo.getTypeInfo(Value.ARRAY, precision, -1, componentType);
    }

    private TypeInfo parseEnumType() {
        read(OPEN_PAREN);
        ArrayList<String> enumeratorList = new ArrayList<>();
        do {
            enumeratorList.add(readString());
        } while (readIfMore());
        return TypeInfo.getTypeInfo(Value.ENUM, -1L, -1, new ExtTypeInfoEnum(enumeratorList.toArray(new String[0])));
    }

    private TypeInfo parseGeometryType() {
        ExtTypeInfoGeometry extTypeInfo;
        if (readIf(OPEN_PAREN)) {
            int type = 0;
            if (currentTokenType != IDENTIFIER || currentTokenQuoted) {
                throw getSyntaxError();
            }
            if (!readIf("GEOMETRY")) {
                try {
                    type = EWKTUtils.parseGeometryType(currentToken);
                    read();
                    if (type / 1_000 == 0 && currentTokenType == IDENTIFIER && !currentTokenQuoted) {
                        type += EWKTUtils.parseDimensionSystem(currentToken) * 1_000;
                        read();
                    }
                } catch (IllegalArgumentException ex) {
                    throw getSyntaxError();
                }
            }
            Integer srid = null;
            if (readIf(COMMA)) {
                srid = readInt();
            }
            read(CLOSE_PAREN);
            extTypeInfo = new ExtTypeInfoGeometry(type, srid);
        } else {
            extTypeInfo = null;
        }
        return TypeInfo.getTypeInfo(Value.GEOMETRY, -1L, -1, extTypeInfo);
    }

    private TypeInfo parseRowType() {
        read(OPEN_PAREN);
        LinkedHashMap<String, TypeInfo> fields = new LinkedHashMap<>();
        do {
            String name = readIdentifier();
            if (fields.putIfAbsent(name, parseDataType()) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, name);
            }
        } while (readIfMore());
        return TypeInfo.getTypeInfo(Value.ROW, -1L, -1, new ExtTypeInfoRow(fields));
    }

    private long readPrecision(int valueType) {
        long p = readPositiveLong();
        if (currentTokenType != IDENTIFIER || currentTokenQuoted) {
            return p;
        }
        if ((valueType == Value.BLOB || valueType == Value.CLOB) && currentToken.length() == 1) {
            long mul;
            /*
             * Convert a-z to A-Z. This method is safe, because only A-Z
             * characters are considered below.
             */
            switch (currentToken.charAt(0) & 0xffdf) {
            case 'K':
                mul = 1L << 10;
                break;
            case 'M':
                mul = 1L << 20;
                break;
            case 'G':
                mul = 1L << 30;
                break;
            case 'T':
                mul = 1L << 40;
                break;
            case 'P':
                mul = 1L << 50;
                break;
            default:
                throw getSyntaxError();
            }
            if (p > Long.MAX_VALUE / mul) {
                throw DbException.getInvalidValueException("precision", p + currentToken);
            }
            p *= mul;
            read();
            if (currentTokenType != IDENTIFIER || currentTokenQuoted) {
                return p;
            }
        }
        switch (valueType) {
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.CLOB:
        case Value.CHAR:
            if (!readIf("CHARACTERS") && !readIf("OCTETS")) {
                if (database.getMode().charAndByteLengthUnits && !readIf("CHAR")) {
                    readIf("BYTE");
                }
            }
        }
        return p;
    }

    private Prepared parseCreate() {
        boolean orReplace = false;
        if (readIf(OR)) {
            read("REPLACE");
            orReplace = true;
        }
        boolean force = readIf("FORCE");
        if (readIf("VIEW")) {
            return parseCreateView(force, orReplace);
        } else if (readIf("ALIAS")) {
            return parseCreateFunctionAlias(force);
        } else if (readIf("SEQUENCE")) {
            return parseCreateSequence();
        } else if (readIf(USER)) {
            return parseCreateUser();
        } else if (readIf("TRIGGER")) {
            return parseCreateTrigger(force);
        } else if (readIf("ROLE")) {
            return parseCreateRole();
        } else if (readIf("SCHEMA")) {
            return parseCreateSchema();
        } else if (readIf("CONSTANT")) {
            return parseCreateConstant();
        } else if (readIf("DOMAIN") || readIf("TYPE") || readIf("DATATYPE")) {
            return parseCreateDomain();
        } else if (readIf("AGGREGATE")) {
            return parseCreateAggregate(force);
        } else if (readIf("LINKED")) {
            return parseCreateLinkedTable(false, false, force);
        }
        // tables or linked tables
        boolean memory = false, cached = false;
        if (readIf("MEMORY")) {
            memory = true;
        } else if (readIf("CACHED")) {
            cached = true;
        }
        if (readIf("LOCAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, false, force);
            }
            read(TABLE);
            return parseCreateTable(true, false, cached);
        } else if (readIf("GLOBAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read(TABLE);
            return parseCreateTable(true, true, cached);
        } else if (readIf("TEMP") || readIf("TEMPORARY")) {
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read(TABLE);
            return parseCreateTable(true, true, cached);
        } else if (readIf(TABLE)) {
            if (!cached && !memory) {
                cached = database.getDefaultTableType() == Table.TYPE_CACHED;
            }
            return parseCreateTable(false, false, cached);
        } else if (readIf("SYNONYM")) {
            return parseCreateSynonym(orReplace);
        } else {
            boolean hash = false, primaryKey = false;
            boolean unique = false, spatial = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if (session.isQuirksMode() && readIf(PRIMARY)) {
                read(KEY);
                if (readIf("HASH")) {
                    hash = true;
                }
                primaryKey = true;
                if (!isToken(ON)) {
                    ifNotExists = readIfNotExists();
                    indexName = readIdentifierWithSchema(null);
                    oldSchema = getSchema();
                }
            } else {
                if (readIf(UNIQUE)) {
                    unique = true;
                }
                if (readIf("HASH")) {
                    hash = true;
                } else if (!unique && readIf("SPATIAL")) {
                    spatial = true;
                }
                read("INDEX");
                if (!isToken(ON)) {
                    ifNotExists = readIfNotExists();
                    indexName = readIdentifierWithSchema(null);
                    oldSchema = getSchema();
                }
            }
            read(ON);
            String tableName = readIdentifierWithSchema();
            checkSchema(oldSchema);
            String comment = readCommentIf();
            if (!readIf(OPEN_PAREN)) {
                // PostgreSQL compatibility
                if (hash || spatial) {
                    throw getSyntaxError();
                }
                read(USING);
                if (readIf("BTREE")) {
                    // default
                } else if (readIf("HASH")) {
                    hash = true;
                } else {
                    read("RTREE");
                    spatial = true;
                }
                read(OPEN_PAREN);
            }
            CreateIndex command = new CreateIndex(session, getSchema());
            command.setIfNotExists(ifNotExists);
            command.setPrimaryKey(primaryKey);
            command.setTableName(tableName);
            command.setHash(hash);
            command.setSpatial(spatial);
            command.setIndexName(indexName);
            command.setComment(comment);
            IndexColumn[] columns;
            int uniqueColumnCount = 0;
            if (spatial) {
                columns = new IndexColumn[] { new IndexColumn(readIdentifier()) };
                if (unique) {
                    uniqueColumnCount = 1;
                }
                read(CLOSE_PAREN);
            } else {
                columns = parseIndexColumnList();
                if (unique) {
                    uniqueColumnCount = columns.length;
                    if (readIf("INCLUDE")) {
                        read(OPEN_PAREN);
                        IndexColumn[] columnsToInclude = parseIndexColumnList();
                        int nonUniqueCount = columnsToInclude.length;
                        columns = Arrays.copyOf(columns, uniqueColumnCount + nonUniqueCount);
                        System.arraycopy(columnsToInclude, 0, columns, uniqueColumnCount, nonUniqueCount);
                    }
                } else if (primaryKey) {
                    uniqueColumnCount = columns.length;
                }
            }
            command.setIndexColumns(columns);
            command.setUniqueColumnCount(uniqueColumnCount);
            return command;
        }
    }

    /**
     * @return true if we expect to see a TABLE clause
     */
    private boolean addRoleOrRight(GrantRevoke command) {
        if (readIf(SELECT)) {
            command.addRight(Right.SELECT);
            return true;
        } else if (readIf("DELETE")) {
            command.addRight(Right.DELETE);
            return true;
        } else if (readIf("INSERT")) {
            command.addRight(Right.INSERT);
            return true;
        } else if (readIf("UPDATE")) {
            command.addRight(Right.UPDATE);
            return true;
        } else if (readIf("CONNECT")) {
            // ignore this right
            return true;
        } else if (readIf("RESOURCE")) {
            // ignore this right
            return true;
        } else {
            command.addRoleName(readIdentifier());
            return false;
        }
    }

    private GrantRevoke parseGrantRevoke(int operationType) {
        GrantRevoke command = new GrantRevoke(session);
        command.setOperationType(operationType);
        boolean tableClauseExpected;
        if (readIf(ALL)) {
            readIf("PRIVILEGES");
            command.addRight(Right.ALL);
            tableClauseExpected = true;
        } else if (readIf("ALTER")) {
            read(ANY);
            read("SCHEMA");
            command.addRight(Right.ALTER_ANY_SCHEMA);
            command.addTable(null);
            tableClauseExpected = false;
        } else {
            tableClauseExpected = addRoleOrRight(command);
            while (readIf(COMMA)) {
                if (addRoleOrRight(command) != tableClauseExpected) {
                    throw DbException.get(ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED);
                }
            }
        }
        if (tableClauseExpected) {
            if (readIf(ON)) {
                if (readIf("SCHEMA")) {
                    command.setSchema(database.getSchema(readIdentifier()));
                } else {
                    readIf(TABLE);
                    do {
                        Table table = readTableOrView();
                        command.addTable(table);
                    } while (readIf(COMMA));
                }
            }
        }
        read(operationType == CommandInterface.GRANT ? TO : FROM);
        command.setGranteeName(readIdentifier());
        return command;
    }

    private TableValueConstructor parseValues() {
        ArrayList<ArrayList<Expression>> rows = Utils.newSmallArrayList();
        ArrayList<Expression> row = parseValuesRow(Utils.newSmallArrayList());
        rows.add(row);
        int columnCount = row.size();
        while (readIf(COMMA)) {
            row = parseValuesRow(new ArrayList<>(columnCount));
            if (row.size() != columnCount) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
            rows.add(row);
        }
        return new TableValueConstructor(session, rows);
    }

    private ArrayList<Expression> parseValuesRow(ArrayList<Expression> row) {
        if (readIf(ROW)) {
            read(OPEN_PAREN);
        } else if (!readIf(OPEN_PAREN)) {
            row.add(readExpression());
            return row;
        }
        do {
            row.add(readExpression());
        } while (readIfMore());
        return row;
    }

    private Call parseCall() {
        Call command = new Call(session);
        currentPrepared = command;
        int index = lastParseIndex;
        boolean canBeFunction;
        switch (currentTokenType) {
        case IDENTIFIER:
            canBeFunction = true;
            break;
        case TABLE:
            read();
            read(OPEN_PAREN);
            command.setTableFunction(readTableFunction(ArrayTableFunction.TABLE));
            return command;
        default:
            canBeFunction = false;
        }
        try {
            command.setExpression(readExpression());
        } catch (DbException e) {
            if (canBeFunction && e.getErrorCode() == ErrorCode.FUNCTION_NOT_FOUND_1) {
                reread(index);
                String schemaName = null, name = readIdentifier();
                if (readIf(DOT)) {
                    schemaName = name;
                    name = readIdentifier();
                    if (readIf(DOT)) {
                        checkDatabaseName(schemaName);
                        schemaName = name;
                        name = readIdentifier();
                    }
                }
                read(OPEN_PAREN);
                Schema schema = schemaName != null ? database.getSchema(schemaName) : null;
                command.setTableFunction(readTableFunction(name, schema));
                return command;
            }
            throw e;
        }
        return command;
    }

    private CreateRole parseCreateRole() {
        CreateRole command = new CreateRole(session);
        command.setIfNotExists(readIfNotExists());
        command.setRoleName(readIdentifier());
        return command;
    }

    private CreateSchema parseCreateSchema() {
        CreateSchema command = new CreateSchema(session);
        command.setIfNotExists(readIfNotExists());
        String authorization;
        if (readIf(AUTHORIZATION)) {
            authorization = readIdentifier();
            command.setSchemaName(authorization);
            command.setAuthorization(authorization);
        } else {
            command.setSchemaName(readIdentifier());
            if (readIf(AUTHORIZATION)) {
                authorization = readIdentifier();
            } else {
                authorization = session.getUser().getName();
            }
        }
        command.setAuthorization(authorization);
        if (readIf(WITH)) {
            command.setTableEngineParams(readTableEngineParams());
        }
        return command;
    }

    private ArrayList<String> readTableEngineParams() {
        ArrayList<String> tableEngineParams = Utils.newSmallArrayList();
        do {
            tableEngineParams.add(readIdentifier());
        } while (readIf(COMMA));
        return tableEngineParams;
    }

    private CreateSequence parseCreateSequence() {
        boolean ifNotExists = readIfNotExists();
        String sequenceName = readIdentifierWithSchema();
        CreateSequence command = new CreateSequence(session, getSchema());
        command.setIfNotExists(ifNotExists);
        command.setSequenceName(sequenceName);
        SequenceOptions options = new SequenceOptions();
        parseSequenceOptions(options, command, true, false);
        command.setOptions(options);
        return command;
    }

    private boolean readIfNotExists() {
        if (readIf(IF)) {
            read(NOT);
            read(EXISTS);
            return true;
        }
        return false;
    }

    private CreateConstant parseCreateConstant() {
        boolean ifNotExists = readIfNotExists();
        String constantName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (isKeyword(constantName)) {
            throw DbException.get(ErrorCode.CONSTANT_ALREADY_EXISTS_1,
                    constantName);
        }
        read(VALUE);
        Expression expr = readExpression();
        CreateConstant command = new CreateConstant(session, schema);
        command.setConstantName(constantName);
        command.setExpression(expr);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateAggregate parseCreateAggregate(boolean force) {
        boolean ifNotExists = readIfNotExists();
        String name = readIdentifierWithSchema(), upperName;
        if (isKeyword(name) || BuiltinFunctions.isBuiltinFunction(database, upperName = upperName(name))
                || Aggregate.getAggregateType(upperName) != null) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
        }
        CreateAggregate command = new CreateAggregate(session, getSchema());
        command.setForce(force);
        command.setName(name);
        command.setIfNotExists(ifNotExists);
        read(FOR);
        command.setJavaClassMethod(readStringOrIdentifier());
        return command;
    }

    private CreateDomain parseCreateDomain() {
        boolean ifNotExists = readIfNotExists();
        String domainName = readIdentifierWithSchema();
        Schema schema = getSchema();
        CreateDomain command = new CreateDomain(session, schema);
        command.setIfNotExists(ifNotExists);
        command.setTypeName(domainName);
        readIf(AS);
        TypeInfo dataType = readIfDataType();
        if (dataType != null) {
            command.setDataType(dataType);
        } else {
            String parentDomainName = readIdentifierWithSchema();
            command.setParentDomain(getSchema().getDomain(parentDomainName));
        }
        if (readIf(DEFAULT)) {
            command.setDefaultExpression(readExpression());
        }
        if (readIf(ON)) {
            read("UPDATE");
            command.setOnUpdateExpression(readExpression());
        }
        // Compatibility with 1.4.200 and older versions
        if (readIf("SELECTIVITY")) {
            readNonNegativeInt();
        }
        String comment = readCommentIf();
        if (comment != null) {
            command.setComment(comment);
        }
        for (;;) {
            String constraintName;
            if (readIf(CONSTRAINT)) {
                constraintName = readIdentifier();
                read(CHECK);
            } else if (readIf(CHECK)) {
                constraintName = null;
            } else {
                break;
            }
            AlterDomainAddConstraint constraint = new AlterDomainAddConstraint(session, schema, ifNotExists);
            constraint.setConstraintName(constraintName);
            constraint.setDomainName(domainName);
            parseDomainConstraint = true;
            try {
                constraint.setCheckExpression(readExpression());
            } finally {
                parseDomainConstraint = false;
            }
            command.addConstraintCommand(constraint);
        }
        return command;
    }

    private CreateTrigger parseCreateTrigger(boolean force) {
        boolean ifNotExists = readIfNotExists();
        String triggerName = readIdentifierWithSchema(null);
        Schema schema = getSchema();
        boolean insteadOf, isBefore;
        if (readIf("INSTEAD")) {
            read("OF");
            isBefore = true;
            insteadOf = true;
        } else if (readIf("BEFORE")) {
            insteadOf = false;
            isBefore = true;
        } else {
            read("AFTER");
            insteadOf = false;
            isBefore = false;
        }
        int typeMask = 0;
        boolean onRollback = false;
        boolean allowOr = database.getMode().getEnum() == ModeEnum.PostgreSQL;
        do {
            if (readIf("INSERT")) {
                typeMask |= Trigger.INSERT;
            } else if (readIf("UPDATE")) {
                typeMask |= Trigger.UPDATE;
            } else if (readIf("DELETE")) {
                typeMask |= Trigger.DELETE;
            } else if (readIf(SELECT)) {
                typeMask |= Trigger.SELECT;
            } else if (readIf("ROLLBACK")) {
                onRollback = true;
            } else {
                throw getSyntaxError();
            }
        } while (readIf(COMMA) || allowOr && readIf(OR));
        read(ON);
        String tableName = readIdentifierWithSchema();
        checkSchema(schema);
        CreateTrigger command = new CreateTrigger(session, getSchema());
        command.setForce(force);
        command.setTriggerName(triggerName);
        command.setIfNotExists(ifNotExists);
        command.setInsteadOf(insteadOf);
        command.setBefore(isBefore);
        command.setOnRollback(onRollback);
        command.setTypeMask(typeMask);
        command.setTableName(tableName);
        if (readIf(FOR)) {
            read("EACH");
            if (readIf(ROW)) {
                command.setRowBased(true);
            } else {
                read("STATEMENT");
            }
        }
        if (readIf("QUEUE")) {
            command.setQueueSize(readNonNegativeInt());
        }
        command.setNoWait(readIf("NOWAIT"));
        if (readIf(AS)) {
            command.setTriggerSource(readString());
        } else {
            read("CALL");
            command.setTriggerClassName(readStringOrIdentifier());
        }
        return command;
    }

    private CreateUser parseCreateUser() {
        CreateUser command = new CreateUser(session);
        command.setIfNotExists(readIfNotExists());
        command.setUserName(readIdentifier());
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
            command.setPassword(ValueExpression.get(ValueVarchar.get(readIdentifier())));
        } else {
            throw getSyntaxError();
        }
        if (readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private CreateFunctionAlias parseCreateFunctionAlias(boolean force) {
        boolean ifNotExists = readIfNotExists();
        String aliasName;
        if (currentTokenType != IDENTIFIER) {
            aliasName = currentToken;
            read();
            schemaName = session.getCurrentSchemaName();
        } else {
            aliasName = readIdentifierWithSchema();
        }
        String upperName = upperName(aliasName);
        if (isReservedFunctionName(upperName)) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, aliasName);
        }
        CreateFunctionAlias command = new CreateFunctionAlias(session, getSchema());
        command.setForce(force);
        command.setAliasName(aliasName);
        command.setIfNotExists(ifNotExists);
        command.setDeterministic(readIf("DETERMINISTIC"));
        // Compatibility with old versions of H2
        readIf("NOBUFFER");
        if (readIf(AS)) {
            command.setSource(readString());
        } else {
            read(FOR);
            command.setJavaClassMethod(readStringOrIdentifier());
        }
        return command;
    }

    private String readStringOrIdentifier() {
        return currentTokenType != IDENTIFIER ? readString() : readIdentifier();
    }

    private boolean isReservedFunctionName(String name) {
        int tokenType = ParserUtil.getTokenType(name, false, 0, name.length(), false);
        if (tokenType != ParserUtil.IDENTIFIER) {
            if (database.isAllowBuiltinAliasOverride()) {
                switch (tokenType) {
                case CURRENT_DATE:
                case CURRENT_TIME:
                case CURRENT_TIMESTAMP:
                case DAY:
                case HOUR:
                case LOCALTIME:
                case LOCALTIMESTAMP:
                case MINUTE:
                case MONTH:
                case SECOND:
                case YEAR:
                    return false;
                }
            }
            return true;
        }
        return Aggregate.getAggregateType(name) != null
                || BuiltinFunctions.isBuiltinFunction(database, name) && !database.isAllowBuiltinAliasOverride();
    }

    private Prepared parseWith() {
        List<TableView> viewsCreated = new ArrayList<>();
        try {
            return parseWith1(viewsCreated);
        } catch (Throwable t) {
            CommandContainer.clearCTE(session, viewsCreated);
            throw t;
        }
    }

    private Prepared parseWith1(List<TableView> viewsCreated) {
        readIf("RECURSIVE");

        // This WITH statement is not a temporary view - it is part of a persistent view
        // as in CREATE VIEW abc AS WITH my_cte - this auto detects that condition.
        final boolean isTemporary = !session.isParsingCreateView();

        do {
            viewsCreated.add(parseSingleCommonTableExpression(isTemporary));
        } while (readIf(COMMA));

        Prepared p;
        // Reverse the order of constructed CTE views - as the destruction order
        // (since later created view may depend on previously created views -
        //  we preserve that dependency order in the destruction sequence )
        // used in setCteCleanups.
        Collections.reverse(viewsCreated);

        int parentheses = 0;
        while (readIf(OPEN_PAREN)) {
            parentheses++;
        }
        int start = lastParseIndex;
        if (isToken(SELECT) || isToken(VALUES)) {
            p = parseWithQuery();
        } else if (isToken(TABLE)) {
            int index = lastParseIndex;
            read();
            if (!isToken(OPEN_PAREN)) {
                reread(index);
                p = parseWithQuery();
            } else {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1, WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);
            }
        } else if (readIf("INSERT")) {
            p = parseInsert(start);
            p.setPrepareAlways(true);
        } else if (readIf("UPDATE")) {
            p = parseUpdate(start);
            p.setPrepareAlways(true);
        } else if (readIf("MERGE")) {
            p = parseMerge(start);
            p.setPrepareAlways(true);
        } else if (readIf("DELETE")) {
            p = parseDelete(start);
            p.setPrepareAlways(true);
        } else if (readIf("CREATE")) {
            if (!isToken(TABLE)) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                        WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);

            }
            p = parseCreate();
            p.setPrepareAlways(true);
        } else {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                    WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);
        }
        for (; parentheses > 0; parentheses--) {
            read(CLOSE_PAREN);
        }

        // Clean up temporary views starting with last to first (in case of
        // dependencies) - but only if they are not persistent.
        if (isTemporary) {
            if (cteCleanups == null) {
                cteCleanups = new ArrayList<>(viewsCreated.size());
            }
            cteCleanups.addAll(viewsCreated);
        }
        return p;
    }

    private Prepared parseWithQuery() {
        Query query = parseSelectUnion();
        query.setPrepareAlways(true);
        query.setNeverLazy(true);
        return query;
    }

    private TableView parseSingleCommonTableExpression(boolean isTemporary) {
        String cteViewName = readIdentifierWithSchema();
        Schema schema = getSchema();
        ArrayList<Column> columns = Utils.newSmallArrayList();
        String[] cols = null;

        // column names are now optional - they can be inferred from the named
        // query, if not supplied by user
        if (readIf(OPEN_PAREN)) {
            cols = parseColumnList();
            for (String c : cols) {
                // we don't really know the type of the column, so STRING will
                // have to do, UNKNOWN does not work here
                columns.add(new Column(c, TypeInfo.TYPE_VARCHAR));
            }
        }

        Table oldViewFound;
        if (!isTemporary) {
            oldViewFound = getSchema().findTableOrView(session, cteViewName);
        } else {
            oldViewFound = session.findLocalTempTable(cteViewName);
        }
        // this persistent check conflicts with check 10 lines down
        if (oldViewFound != null) {
            if (!(oldViewFound instanceof TableView)) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                        cteViewName);
            }
            TableView tv = (TableView) oldViewFound;
            if (!tv.isTableExpression()) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                        cteViewName);
            }
            if (!isTemporary) {
                oldViewFound.lock(session, true, true);
                database.removeSchemaObject(session, oldViewFound);

            } else {
                session.removeLocalTempTable(oldViewFound);
            }
        }
        /*
         * This table is created as a workaround because recursive table
         * expressions need to reference something that look like themselves to
         * work (its removed after creation in this method). Only create table
         * data and table if we don't have a working CTE already.
         */
        Table recursiveTable = TableView.createShadowTableForRecursiveTableExpression(
                isTemporary, session, cteViewName, schema, columns, database);
        List<Column> columnTemplateList;
        String[] querySQLOutput = new String[1];
        try {
            read(AS);
            read(OPEN_PAREN);
            Query withQuery = parseQuery();
            if (!isTemporary) {
                withQuery.session = session;
            }
            read(CLOSE_PAREN);
            columnTemplateList = TableView.createQueryColumnTemplateList(cols, withQuery, querySQLOutput);

        } finally {
            TableView.destroyShadowTableForRecursiveExpression(isTemporary, session, recursiveTable);
        }

        return createCTEView(cteViewName,
                querySQLOutput[0], columnTemplateList,
                true/* allowRecursiveQueryDetection */,
                true/* add to session */,
                isTemporary);
    }

    private TableView createCTEView(String cteViewName, String querySQL,
                                    List<Column> columnTemplateList, boolean allowRecursiveQueryDetection,
                                    boolean addViewToSession, boolean isTemporary) {
        Schema schema = getSchemaWithDefault();
        int id = database.allocateObjectId();
        Column[] columnTemplateArray = columnTemplateList.toArray(new Column[0]);

        // No easy way to determine if this is a recursive query up front, so we just compile
        // it twice - once without the flag set, and if we didn't see a recursive term,
        // then we just compile it again.
        TableView view;
        synchronized (session) {
            view = new TableView(schema, id, cteViewName, querySQL,
                    parameters, columnTemplateArray, session,
                    allowRecursiveQueryDetection, false /* literalsChecked */, true /* isTableExpression */,
                    isTemporary);
            if (!view.isRecursiveQueryDetected() && allowRecursiveQueryDetection) {
                if (!isTemporary) {
                    database.addSchemaObject(session, view);
                    view.lock(session, true, true);
                    database.removeSchemaObject(session, view);
                } else {
                    session.removeLocalTempTable(view);
                }
                view = new TableView(schema, id, cteViewName, querySQL, parameters,
                        columnTemplateArray, session,
                        false/* assume recursive */, false /* literalsChecked */, true /* isTableExpression */,
                        isTemporary);
            }
            // both removeSchemaObject and removeLocalTempTable hold meta locks
            database.unlockMeta(session);
        }
        view.setTableExpression(true);
        view.setTemporary(isTemporary);
        view.setHidden(true);
        view.setOnCommitDrop(false);
        if (addViewToSession) {
            if (!isTemporary) {
                database.addSchemaObject(session, view);
                view.unlock(session);
                database.unlockMeta(session);
            } else {
                session.addLocalTempTable(view);
            }
        }
        return view;
    }

    private CreateView parseCreateView(boolean force, boolean orReplace) {
        boolean ifNotExists = readIfNotExists();
        boolean isTableExpression = readIf("TABLE_EXPRESSION");
        String viewName = readIdentifierWithSchema();
        CreateView command = new CreateView(session, getSchema());
        this.createView = command;
        command.setViewName(viewName);
        command.setIfNotExists(ifNotExists);
        command.setComment(readCommentIf());
        command.setOrReplace(orReplace);
        command.setForce(force);
        command.setTableExpression(isTableExpression);
        if (readIf(OPEN_PAREN)) {
            String[] cols = parseColumnList();
            command.setColumnNames(cols);
        }
        String select = StringUtils.cache(sqlCommand
                .substring(parseIndex));
        read(AS);
        try {
            Query query;
            session.setParsingCreateView(true);
            try {
                query = parseQuery();
                query.prepare();
            } finally {
                session.setParsingCreateView(false);
            }
            command.setSelect(query);
        } catch (DbException e) {
            if (force) {
                command.setSelectSQL(select);
                while (currentTokenType != END_OF_INPUT) {
                    read();
                }
            } else {
                throw e;
            }
        }
        return command;
    }

    private TransactionCommand parseCheckpoint() {
        TransactionCommand command;
        if (readIf("SYNC")) {
            command = new TransactionCommand(session,
                    CommandInterface.CHECKPOINT_SYNC);
        } else {
            command = new TransactionCommand(session,
                    CommandInterface.CHECKPOINT);
        }
        return command;
    }

    private Prepared parseAlter() {
        if (readIf(TABLE)) {
            return parseAlterTable();
        } else if (readIf(USER)) {
            return parseAlterUser();
        } else if (readIf("INDEX")) {
            return parseAlterIndex();
        } else if (readIf("SCHEMA")) {
            return parseAlterSchema();
        } else if (readIf("SEQUENCE")) {
            return parseAlterSequence();
        } else if (readIf("VIEW")) {
            return parseAlterView();
        } else if (readIf("DOMAIN")) {
            return parseAlterDomain();
        }
        throw getSyntaxError();
    }

    private void checkSchema(Schema old) {
        if (old != null && getSchema() != old) {
            throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH);
        }
    }

    private AlterIndexRename parseAlterIndex() {
        boolean ifExists = readIfExists(false);
        String indexName = readIdentifierWithSchema();
        Schema old = getSchema();
        AlterIndexRename command = new AlterIndexRename(session);
        command.setOldSchema(old);
        command.setOldName(indexName);
        command.setIfExists(ifExists);
        read("RENAME");
        read(TO);
        String newName = readIdentifierWithSchema(old.getName());
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private DefineCommand parseAlterDomain() {
        boolean ifDomainExists = readIfExists(false);
        String domainName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (readIf("ADD")) {
            boolean ifNotExists = false;
            String constraintName = null;
            String comment = null;
            if (readIf(CONSTRAINT)) {
                ifNotExists = readIfNotExists();
                constraintName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                comment = readCommentIf();
            }
            read(CHECK);
            AlterDomainAddConstraint command = new AlterDomainAddConstraint(session, schema, ifNotExists);
            command.setDomainName(domainName);
            command.setConstraintName(constraintName);
            parseDomainConstraint = true;
            try {
                command.setCheckExpression(readExpression());
            } finally {
                parseDomainConstraint = false;
            }
            command.setIfDomainExists(ifDomainExists);
            command.setComment(comment);
            if (readIf("NOCHECK")) {
                command.setCheckExisting(false);
            } else {
                readIf(CHECK);
                command.setCheckExisting(true);
            }
            return command;
        } else if (readIf("DROP")) {
            if (readIf(CONSTRAINT)) {
                boolean ifConstraintExists = readIfExists(false);
                String constraintName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                AlterDomainDropConstraint command = new AlterDomainDropConstraint(session, getSchema(),
                        ifConstraintExists);
                command.setConstraintName(constraintName);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                return command;
            } else if (readIf(DEFAULT)) {
                AlterDomainExpressions command = new AlterDomainExpressions(session, schema,
                        CommandInterface.ALTER_DOMAIN_DEFAULT);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                command.setExpression(null);
                return command;
            } else if (readIf(ON)) {
                read("UPDATE");
                AlterDomainExpressions command = new AlterDomainExpressions(session, schema,
                        CommandInterface.ALTER_DOMAIN_ON_UPDATE);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                command.setExpression(null);
                return command;
            }
        } else if (readIf("RENAME")) {
            if (readIf(CONSTRAINT)) {
                String constraintName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                read(TO);
                AlterDomainRenameConstraint command = new AlterDomainRenameConstraint(session, schema);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                command.setConstraintName(constraintName);
                command.setNewConstraintName(readIdentifier());
                return command;
            }
            read(TO);
            String newName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            AlterDomainRename command = new AlterDomainRename(session, getSchema());
            command.setDomainName(domainName);
            command.setIfDomainExists(ifDomainExists);
            command.setNewDomainName(newName);
            return command;
        } else {
            read(SET);
            if (readIf(DEFAULT)) {
                AlterDomainExpressions command = new AlterDomainExpressions(session, schema,
                        CommandInterface.ALTER_DOMAIN_DEFAULT);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                command.setExpression(readExpression());
                return command;
            } else if (readIf(ON)) {
                read("UPDATE");
                AlterDomainExpressions command = new AlterDomainExpressions(session, schema,
                        CommandInterface.ALTER_DOMAIN_ON_UPDATE);
                command.setDomainName(domainName);
                command.setIfDomainExists(ifDomainExists);
                command.setExpression(readExpression());
                return command;
            }
        }
        throw getSyntaxError();
    }

    private DefineCommand parseAlterView() {
        boolean ifExists = readIfExists(false);
        String viewName = readIdentifierWithSchema();
        Schema schema = getSchema();
        Table tableView = schema.findTableOrView(session, viewName);
        if (!(tableView instanceof TableView) && !ifExists) {
            throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
        }
        if (readIf("RENAME")) {
            read(TO);
            String newName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            AlterTableRename command = new AlterTableRename(session, getSchema());
            command.setTableName(viewName);
            command.setNewTableName(newName);
            command.setIfTableExists(ifExists);
            return command;
        } else {
            read("RECOMPILE");
            TableView view = (TableView) tableView;
            AlterView command = new AlterView(session);
            command.setIfExists(ifExists);
            command.setView(view);
            return command;
        }
    }

    private Prepared parseAlterSchema() {
        boolean ifExists = readIfExists(false);
        String schemaName = readIdentifierWithSchema();
        Schema old = getSchema();
        read("RENAME");
        read(TO);
        String newName = readIdentifierWithSchema(old.getName());
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            if (ifExists) {
                return new NoOperation(session);
            }
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        AlterSchemaRename command = new AlterSchemaRename(session);
        command.setOldSchema(schema);
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private AlterSequence parseAlterSequence() {
        boolean ifExists = readIfExists(false);
        String sequenceName = readIdentifierWithSchema();
        AlterSequence command = new AlterSequence(session, getSchema());
        command.setSequenceName(sequenceName);
        command.setIfExists(ifExists);
        SequenceOptions options = new SequenceOptions();
        parseSequenceOptions(options, null, false, false);
        command.setOptions(options);
        return command;
    }

    private boolean parseSequenceOptions(SequenceOptions options, CreateSequence command, boolean allowDataType,
            boolean forAlterColumn) {
        boolean result = false;
        for (;;) {
            if (allowDataType && readIf(AS)) {
                TypeInfo dataType = parseDataType();
                if (!DataType.isNumericType(dataType.getValueType())) {
                    throw DbException.getUnsupportedException(dataType
                            .getSQL(new StringBuilder("CREATE SEQUENCE AS "), HasSQL.TRACE_SQL_FLAGS).toString());
                }
                options.setDataType(dataType);
            } else if (readIf("START")) {
                read(WITH);
                options.setStartValue(readExpression());
            } else if (readIf("RESTART")) {
                options.setRestartValue(readIf(WITH) ? readExpression() : ValueExpression.DEFAULT);
            } else if (command != null && parseCreateSequenceOption(command)) {
                //
            } else if (forAlterColumn) {
                int index = lastParseIndex;
                if (readIf(SET)) {
                    if (!parseBasicSequenceOption(options)) {
                        reread(index);
                        break;
                    }
                } else {
                    break;
                }
            } else if (!parseBasicSequenceOption(options)) {
                break;
            }
            result = true;
        }
        return result;
    }

    private boolean parseCreateSequenceOption(CreateSequence command) {
        if (readIf("BELONGS_TO_TABLE")) {
            command.setBelongsToTable(true);
        } else if (readIf(ORDER)) {
            // Oracle compatibility
        } else {
            return false;
        }
        return true;
    }

    private boolean parseBasicSequenceOption(SequenceOptions options) {
        if (readIf("INCREMENT")) {
            readIf("BY");
            options.setIncrement(readExpression());
        } else if (readIf("MINVALUE")) {
            options.setMinValue(readExpression());
        } else if (readIf("MAXVALUE")) {
            options.setMaxValue(readExpression());
        } else if (readIf("CYCLE")) {
            options.setCycle(Sequence.Cycle.CYCLE);
        } else if (readIf("NO")) {
            if (readIf("MINVALUE")) {
                options.setMinValue(ValueExpression.NULL);
            } else if (readIf("MAXVALUE")) {
                options.setMaxValue(ValueExpression.NULL);
            } else if (readIf("CYCLE")) {
                options.setCycle(Sequence.Cycle.NO_CYCLE);
            } else if (readIf("CACHE")) {
                options.setCacheSize(ValueExpression.get(ValueBigint.get(1)));
            } else {
                throw getSyntaxError();
            }
        } else if (readIf("EXHAUSTED")) {
            options.setCycle(Sequence.Cycle.EXHAUSTED);
        } else if (readIf("CACHE")) {
            options.setCacheSize(readExpression());
            // Various compatibility options
        } else if (readIf("NOMINVALUE")) {
            options.setMinValue(ValueExpression.NULL);
        } else if (readIf("NOMAXVALUE")) {
            options.setMaxValue(ValueExpression.NULL);
        } else if (readIf("NOCYCLE")) {
            options.setCycle(Sequence.Cycle.NO_CYCLE);
        } else if (readIf("NOCACHE")) {
            options.setCacheSize(ValueExpression.get(ValueBigint.get(1)));
        } else {
            return false;
        }
        return true;
    }

    private AlterUser parseAlterUser() {
        String userName = readIdentifier();
        if (readIf(SET)) {
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
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
            read(TO);
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_RENAME);
            command.setUser(database.getUser(userName));
            command.setNewName(readIdentifier());
            return command;
        } else if (readIf("ADMIN")) {
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_ADMIN);
            User user = database.getUser(userName);
            command.setUser(user);
            if (readIf(TRUE)) {
                command.setAdmin(true);
            } else if (readIf(FALSE)) {
                command.setAdmin(false);
            } else {
                throw getSyntaxError();
            }
            return command;
        }
        throw getSyntaxError();
    }

    private void readIfEqualOrTo() {
        if (!readIf(EQUAL)) {
            readIf(TO);
        }
    }

    private Prepared parseSet() {
        if (readIf(AT)) {
            Set command = new Set(session, SetTypes.VARIABLE);
            command.setString(readIdentifier());
            readIfEqualOrTo();
            command.setExpression(readExpression());
            return command;
        } else if (readIf("AUTOCOMMIT")) {
            readIfEqualOrTo();
            return new TransactionCommand(session, readBooleanSetting() ? CommandInterface.SET_AUTOCOMMIT_TRUE
                    : CommandInterface.SET_AUTOCOMMIT_FALSE);
        } else if (readIf("EXCLUSIVE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.EXCLUSIVE);
            command.setExpression(readExpression());
            return command;
        } else if (readIf("IGNORECASE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.IGNORECASE);
            command.setInt(readBooleanSetting() ? 1 : 0);
            return command;
        } else if (readIf("PASSWORD")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
            command.setUser(session.getUser());
            command.setPassword(readExpression());
            return command;
        } else if (readIf("SALT")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
            command.setUser(session.getUser());
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
            return command;
        } else if (readIf("MODE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.MODE);
            command.setString(readIdentifier());
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
            int v;
            if (readIf(ALL)) {
                v = Constants.ALLOW_LITERALS_ALL;
            } else if (readIf("NONE")) {
                v = Constants.ALLOW_LITERALS_NONE;
            } else if (readIf("NUMBERS")) {
                v = Constants.ALLOW_LITERALS_NUMBERS;
            } else {
                v = readNonNegativeInt();
            }
            command.setInt(v);
            return command;
        } else if (readIf("DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DEFAULT_TABLE_TYPE);
            int v;
            if (readIf("MEMORY")) {
                v = Table.TYPE_MEMORY;
            } else if (readIf("CACHED")) {
                v = Table.TYPE_CACHED;
            } else {
                v = readNonNegativeInt();
            }
            command.setInt(v);
            return command;
        } else if (readIf("SCHEMA")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA);
            command.setExpression(readExpressionOrIdentifier());
            return command;
        } else if (readIf("CATALOG")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.CATALOG);
            command.setExpression(readExpressionOrIdentifier());
            return command;
        } else if (readIf(SetTypes.getTypeName(SetTypes.SCHEMA_SEARCH_PATH))) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA_SEARCH_PATH);
            ArrayList<String> list = Utils.newSmallArrayList();
            do {
                list.add(readIdentifier());
            } while (readIf(COMMA));
            command.setStringArray(list.toArray(new String[0]));
            return command;
        } else if (readIf("JAVA_OBJECT_SERIALIZER")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.JAVA_OBJECT_SERIALIZER);
            command.setString(readString());
            return command;
        } else if (readIf("IGNORE_CATALOGS")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.IGNORE_CATALOGS);
            command.setInt(readBooleanSetting() ? 1 : 0);
            return command;
        } else if (readIf("SESSION")) {
            read("CHARACTERISTICS");
            read(AS);
            read("TRANSACTION");
            return parseSetTransactionMode();
        } else if (readIf("TRANSACTION")) {
            // TODO should affect only the current transaction
            return parseSetTransactionMode();
        } else if (readIf("TIME")) {
            read("ZONE");
            Set command = new Set(session, SetTypes.TIME_ZONE);
            if (!readIf("LOCAL")) {
                command.setExpression(readExpression());
            }
            return command;
        } else if (readIf("NON_KEYWORDS")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.NON_KEYWORDS);
            ArrayList<String> list = Utils.newSmallArrayList();
            if (currentTokenType != END_OF_INPUT && currentTokenType != SEMICOLON) {
                do {
                    if (currentTokenType < IDENTIFIER || currentTokenType > LAST_KEYWORD) {
                        throw getSyntaxError();
                    }
                    list.add(StringUtils.toUpperEnglish(currentToken));
                    read();
                } while (readIf(COMMA));
            }
            command.setStringArray(list.toArray(new String[0]));
            return command;
        } else if (readIf("DEFAULT_NULL_ORDERING")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DEFAULT_NULL_ORDERING);
            command.setString(readIdentifier());
            return command;
        } else if (readIf("LOG")) {
            throw DbException.getUnsupportedException("LOG");
        } else {
            String upperName = upperName(currentToken);
            if (ConnectionInfo.isIgnoredByParser(upperName)) {
                read();
                readIfEqualOrTo();
                read();
                return new NoOperation(session);
            }
            int type = SetTypes.getType(upperName);
            if (type >= 0) {
                read();
                readIfEqualOrTo();
                Set command = new Set(session, type);
                command.setExpression(readExpression());
                return command;
            }
            ModeEnum modeEnum = database.getMode().getEnum();
            if (modeEnum != ModeEnum.REGULAR) {
                Prepared command = readSetCompatibility(modeEnum);
                if (command != null) {
                    return command;
                }
            }
            if (session.isQuirksMode()) {
                switch (upperName) {
                case "BINARY_COLLATION":
                case "UUID_COLLATION":
                    read();
                    readIfEqualOrTo();
                    readIdentifier();
                    return new NoOperation(session);
                }
            }
            throw getSyntaxError();
        }
    }

    private Prepared parseSetTransactionMode() {
        IsolationLevel isolationLevel;
        read("ISOLATION");
        read("LEVEL");
        if (readIf("READ")) {
            if (readIf("UNCOMMITTED")) {
                isolationLevel = IsolationLevel.READ_UNCOMMITTED;
            } else {
                read("COMMITTED");
                isolationLevel = IsolationLevel.READ_COMMITTED;
            }
        } else if (readIf("REPEATABLE")) {
            read("READ");
            isolationLevel = IsolationLevel.REPEATABLE_READ;
        } else if (readIf("SNAPSHOT")) {
            isolationLevel = IsolationLevel.SNAPSHOT;
        } else {
            read("SERIALIZABLE");
            isolationLevel = IsolationLevel.SERIALIZABLE;
        }
        return new SetSessionCharacteristics(session, isolationLevel);
    }

    private Expression readExpressionOrIdentifier() {
        if (isIdentifier()) {
            return ValueExpression.get(ValueVarchar.get(readIdentifier()));
        }
        return readExpression();
    }

    private Prepared parseUse() {
        readIfEqualOrTo();
        Set command = new Set(session, SetTypes.SCHEMA);
        command.setExpression(ValueExpression.get(ValueVarchar.get(readIdentifier())));
        return command;
    }

    private Set parseSetCollation() {
        Set command = new Set(session, SetTypes.COLLATION);
        String name = readIdentifier();
        command.setString(name);
        if (equalsToken(name, CompareMode.OFF)) {
            return command;
        }
        Collator coll = CompareMode.getCollator(name);
        if (coll == null) {
            throw DbException.getInvalidValueException("collation", name);
        }
        if (readIf("STRENGTH")) {
            if (readIf(PRIMARY)) {
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

    private Prepared readSetCompatibility(ModeEnum modeEnum) {
        switch (modeEnum) {
        case Derby:
            if (readIf("CREATE")) {
                readIfEqualOrTo();
                // (CREATE=TRUE in the database URL)
                read();
                return new NoOperation(session);
            }
            break;
        case HSQLDB:
            if (readIf("LOGSIZE")) {
                readIfEqualOrTo();
                Set command = new Set(session, SetTypes.MAX_LOG_SIZE);
                command.setExpression(readExpression());
                return command;
            }
            break;
        case MySQL:
            if (readIf("FOREIGN_KEY_CHECKS")) {
                readIfEqualOrTo();
                Set command = new Set(session, SetTypes.REFERENTIAL_INTEGRITY);
                command.setExpression(readExpression());
                return command;
            } else if (readIf("NAMES")) {
                // Quercus PHP MySQL driver compatibility
                readIfEqualOrTo();
                read();
                return new NoOperation(session);
            }
            break;
        case PostgreSQL:
            if (readIf("STATEMENT_TIMEOUT")) {
                readIfEqualOrTo();
                Set command = new Set(session, SetTypes.QUERY_TIMEOUT);
                command.setInt(readNonNegativeInt());
                return command;
            } else if (readIf("CLIENT_ENCODING") || readIf("CLIENT_MIN_MESSAGES") || readIf("JOIN_COLLAPSE_LIMIT")) {
                readIfEqualOrTo();
                read();
                return new NoOperation(session);
            } else if (readIf("DATESTYLE")) {
                readIfEqualOrTo();
                if (!readIf("ISO")) {
                    String s = readString();
                    if (!equalsToken(s, "ISO")) {
                        throw getSyntaxError();
                    }
                }
                return new NoOperation(session);
            } else if (readIf("SEARCH_PATH")) {
                readIfEqualOrTo();
                Set command = new Set(session, SetTypes.SCHEMA_SEARCH_PATH);
                ArrayList<String> list = Utils.newSmallArrayList();
                String pgCatalog = database.sysIdentifier("PG_CATALOG");
                boolean hasPgCatalog = false;
                do {
                    // some PG clients will send single-quoted alias
                    String s = currentTokenType == LITERAL ? readString() : readIdentifier();
                    if ("$user".equals(s)) {
                        continue;
                    }
                    if (pgCatalog.equals(s)) {
                        hasPgCatalog = true;
                    }
                    list.add(s);
                } while (readIf(COMMA));
                // If "pg_catalog" is not in the path then it will be searched before
                // searching any of the path items. See
                // https://www.postgresql.org/docs/8.2/runtime-config-client.html
                if (!hasPgCatalog) {
                    if (database.findSchema(pgCatalog) != null) {
                        list.add(0, pgCatalog);
                    }
                }
                command.setStringArray(list.toArray(new String[0]));
                return command;
            }
            break;
        default:
        }
        return null;
    }

    private RunScriptCommand parseRunScript() {
        RunScriptCommand command = new RunScriptCommand(session);
        read(FROM);
        command.setFileNameExpr(readExpression());
        if (readIf("COMPRESSION")) {
            command.setCompressionAlgorithm(readIdentifier());
        }
        if (readIf("CIPHER")) {
            command.setCipher(readIdentifier());
            if (readIf("PASSWORD")) {
                command.setPassword(readExpression());
            }
        }
        if (readIf("CHARSET")) {
            command.setCharset(Charset.forName(readString()));
        }
        if (readIf("FROM_1X")) {
            command.setFrom1X();
        } else {
            if (readIf("QUIRKS_MODE")) {
                command.setQuirksMode(true);
            }
            if (readIf("VARIABLE_BINARY")) {
                command.setVariableBinary(true);
            }
        }
        return command;
    }

    private ScriptCommand parseScript() {
        ScriptCommand command = new ScriptCommand(session);
        boolean data = true, passwords = true, settings = true, version = true;
        boolean dropTables = false, simple = false, withColumns = false;
        if (readIf("NODATA")) {
            data = false;
        } else {
            if (readIf("SIMPLE")) {
                simple = true;
            }
            if (readIf("COLUMNS")) {
                withColumns = true;
            }
        }
        if (readIf("NOPASSWORDS")) {
            passwords = false;
        }
        if (readIf("NOSETTINGS")) {
            settings = false;
        }
        if (readIf("NOVERSION")) {
            version = false;
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
        command.setVersion(version);
        command.setDrop(dropTables);
        command.setSimple(simple);
        command.setWithColumns(withColumns);
        if (readIf(TO)) {
            command.setFileNameExpr(readExpression());
            if (readIf("COMPRESSION")) {
                command.setCompressionAlgorithm(readIdentifier());
            }
            if (readIf("CIPHER")) {
                command.setCipher(readIdentifier());
                if (readIf("PASSWORD")) {
                    command.setPassword(readExpression());
                }
            }
            if (readIf("CHARSET")) {
                command.setCharset(Charset.forName(readString()));
            }
        }
        if (readIf("SCHEMA")) {
            HashSet<String> schemaNames = new HashSet<>();
            do {
                schemaNames.add(readIdentifier());
            } while (readIf(COMMA));
            command.setSchemaNames(schemaNames);
        } else if (readIf(TABLE)) {
            ArrayList<Table> tables = Utils.newSmallArrayList();
            do {
                tables.add(readTableOrView());
            } while (readIf(COMMA));
            command.setTables(tables);
        }
        return command;
    }

    /**
     * Is this the Oracle DUAL table or the IBM/DB2 SYSIBM table?
     *
     * @param tableName table name.
     * @return {@code true} if the table is DUAL special table. Otherwise returns {@code false}.
     * @see <a href="https://en.wikipedia.org/wiki/DUAL_table">Wikipedia: DUAL table</a>
     */
    private boolean isDualTable(String tableName) {
        return ((schemaName == null || equalsToken(schemaName, "SYS")) && equalsToken("DUAL", tableName))
                || (database.getMode().sysDummy1 && (schemaName == null || equalsToken(schemaName, "SYSIBM"))
                        && equalsToken("SYSDUMMY1", tableName));
    }

    private Table readTableOrView() {
        return readTableOrView(readIdentifierWithSchema(null));
    }

    private Table readTableOrView(String tableName) {
        if (schemaName != null) {
            Table table = getSchema().resolveTableOrView(session, tableName);
            if (table != null) {
                return table;
            }
        } else {
            Table table = database.getSchema(session.getCurrentSchemaName())
                    .resolveTableOrView(session, tableName);
            if (table != null) {
                return table;
            }
            String[] schemaNames = session.getSchemaSearchPath();
            if (schemaNames != null) {
                for (String name : schemaNames) {
                    Schema s = database.getSchema(name);
                    table = s.resolveTableOrView(session, tableName);
                    if (table != null) {
                        return table;
                    }
                }
            }
        }
        if (isDualTable(tableName)) {
            return new DualTable(database);
        }

        throw getTableOrViewNotFoundDbException(tableName);
    }

    private DbException getTableOrViewNotFoundDbException(String tableName) {
        if (schemaName != null) {
            return getTableOrViewNotFoundDbException(schemaName, tableName);
        }

        String currentSchemaName = session.getCurrentSchemaName();
        String[] schemaSearchPath = session.getSchemaSearchPath();
        if (schemaSearchPath == null) {
            return getTableOrViewNotFoundDbException(Collections.singleton(currentSchemaName), tableName);
        }

        LinkedHashSet<String> schemaNames = new LinkedHashSet<>();
        schemaNames.add(currentSchemaName);
        schemaNames.addAll(Arrays.asList(schemaSearchPath));
        return getTableOrViewNotFoundDbException(schemaNames, tableName);
    }

    private DbException getTableOrViewNotFoundDbException(String schemaName, String tableName) {
        return getTableOrViewNotFoundDbException(Collections.singleton(schemaName), tableName);
    }

    private DbException getTableOrViewNotFoundDbException(
            java.util.Set<String> schemaNames, String tableName) {
        if (database == null || database.getFirstUserTable() == null) {
            return DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1, tableName);
        }

        if (database.getSettings().caseInsensitiveIdentifiers) {
            return DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }

        java.util.Set<String> candidates = new TreeSet<>();
        for (String schemaName : schemaNames) {
            findTableNameCandidates(schemaName, tableName, candidates);
        }

        if (candidates.isEmpty()) {
            return DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }

        return DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_WITH_CANDIDATES_2,
                tableName,
                String.join(", ", candidates));
    }

    private void findTableNameCandidates(String schemaName, String tableName, java.util.Set<String> candidates) {
        Schema schema = database.getSchema(schemaName);
        String ucTableName = StringUtils.toUpperEnglish(tableName);
        Collection<Table> allTablesAndViews = schema.getAllTablesAndViews(session);
        for (Table candidate : allTablesAndViews) {
            String candidateName = candidate.getName();
            if (ucTableName.equals(StringUtils.toUpperEnglish(candidateName))) {
                candidates.add(candidateName);
            }
        }
    }

    private UserDefinedFunction findUserDefinedFunctionWithinPath(Schema schema, String name) {
        if (schema != null) {
            return schema.findFunctionOrAggregate(name);
        }
        schema = database.getSchema(session.getCurrentSchemaName());
        UserDefinedFunction userDefinedFunction = schema.findFunctionOrAggregate(name);
        if (userDefinedFunction != null) {
            return userDefinedFunction;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        if (schemaNames != null) {
            for (String schemaName : schemaNames) {
                Schema schemaFromPath = database.getSchema(schemaName);
                if (schemaFromPath != schema) {
                    userDefinedFunction = schemaFromPath.findFunctionOrAggregate(name);
                    if (userDefinedFunction != null) {
                        return userDefinedFunction;
                    }
                }
            }
        }
        return null;
    }

    private Sequence findSequence(String schema, String sequenceName) {
        Sequence sequence = database.getSchema(schema).findSequence(
                sequenceName);
        if (sequence != null) {
            return sequence;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        if (schemaNames != null) {
            for (String n : schemaNames) {
                sequence = database.getSchema(n).findSequence(sequenceName);
                if (sequence != null) {
                    return sequence;
                }
            }
        }
        return null;
    }

    private Sequence readSequence() {
        // same algorithm as readTableOrView
        String sequenceName = readIdentifierWithSchema(null);
        if (schemaName != null) {
            return getSchema().getSequence(sequenceName);
        }
        Sequence sequence = findSequence(session.getCurrentSchemaName(),
                sequenceName);
        if (sequence != null) {
            return sequence;
        }
        throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
    }

    private Prepared parseAlterTable() {
        boolean ifTableExists = readIfExists(false);
        String tableName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (readIf("ADD")) {
            Prepared command = parseAlterTableAddConstraintIf(tableName, schema, ifTableExists);
            if (command != null) {
                return command;
            }
            return parseAlterTableAddColumn(tableName, schema, ifTableExists);
        } else if (readIf(SET)) {
            return parseAlterTableSet(schema, tableName, ifTableExists);
        } else if (readIf("RENAME")) {
            return parseAlterTableRename(schema, tableName, ifTableExists);
        } else if (readIf("DROP")) {
            return parseAlterTableDrop(schema, tableName, ifTableExists);
        } else if (readIf("ALTER")) {
            return parseAlterTableAlter(schema, tableName, ifTableExists);
        } else {
            Mode mode = database.getMode();
            if (mode.alterTableExtensionsMySQL || mode.alterTableModifyColumn) {
                return parseAlterTableCompatibility(schema, tableName, ifTableExists, mode);
            }
        }
        throw getSyntaxError();
    }

    private Prepared parseAlterTableAlter(Schema schema, String tableName, boolean ifTableExists) {
        readIf("COLUMN");
        boolean ifExists = readIfExists(false);
        String columnName = readIdentifier();
        Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists, ifExists);
        if (readIf("RENAME")) {
            read(TO);
            AlterTableRenameColumn command = new AlterTableRenameColumn(
                    session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setIfExists(ifExists);
            command.setOldColumnName(columnName);
            String newName = readIdentifier();
            command.setNewColumnName(newName);
            return command;
        } else if (readIf("DROP")) {
            if (readIf(DEFAULT)) {
                if (readIf(ON)) {
                    read(NULL);
                    AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
                    command.setTableName(tableName);
                    command.setIfTableExists(ifTableExists);
                    command.setOldColumn(column);
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT_ON_NULL);
                    command.setBooleanFlag(false);
                    return command;
                }
                return getAlterTableAlterColumnDropDefaultExpression(schema, tableName, ifTableExists, column,
                        CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT);
            } else if (readIf("EXPRESSION")) {
                return getAlterTableAlterColumnDropDefaultExpression(schema, tableName, ifTableExists, column,
                        CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_EXPRESSION);
            } else if (readIf("IDENTITY")) {
                return getAlterTableAlterColumnDropDefaultExpression(schema, tableName, ifTableExists, column,
                        CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_IDENTITY);
            }
            if (readIf(ON)) {
                read("UPDATE");
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumn(column);
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_ON_UPDATE);
                command.setDefaultExpression(null);
                return command;
            }
            read(NOT);
            read(NULL);
            AlterTableAlterColumn command = new AlterTableAlterColumn(
                    session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setOldColumn(column);
            command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
            return command;
        } else if (readIf("TYPE")) {
            // PostgreSQL compatibility
            return parseAlterTableAlterColumnDataType(schema, tableName, columnName, ifTableExists, ifExists);
        } else if (readIf("SELECTIVITY")) {
            AlterTableAlterColumn command = new AlterTableAlterColumn(
                    session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY);
            command.setOldColumn(column);
            command.setSelectivity(readExpression());
            return command;
        }
        Prepared command = parseAlterTableAlterColumnIdentity(schema, tableName, ifTableExists, column);
        if (command != null) {
            return command;
        }
        if (readIf(SET)) {
            return parseAlterTableAlterColumnSet(schema, tableName, ifTableExists, ifExists, columnName, column);
        }
        return parseAlterTableAlterColumnType(schema, tableName, columnName, ifTableExists, ifExists, true);
    }

    private Prepared getAlterTableAlterColumnDropDefaultExpression(Schema schema, String tableName,
            boolean ifTableExists, Column column, int type) {
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setOldColumn(column);
        command.setType(type);
        command.setDefaultExpression(null);
        return command;
    }

    private Prepared parseAlterTableAlterColumnIdentity(Schema schema, String tableName, boolean ifTableExists,
            Column column) {
        int index = lastParseIndex;
        Boolean always = null;
        if (readIf(SET) && readIf("GENERATED")) {
            if (readIf("ALWAYS")) {
                always = true;
            } else {
                read("BY");
                read(DEFAULT);
                always = false;
            }
        } else {
            reread(index);
        }
        SequenceOptions options = new SequenceOptions();
        if (!parseSequenceOptions(options, null, false, true) && always == null) {
            return null;
        }
        if (column == null) {
            return new NoOperation(session);
        }
        if (!column.isIdentity()) {
            AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
            parseAlterColumnUsingIf(command);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE);
            command.setOldColumn(column);
            Column newColumn = column.getClone();
            newColumn.setIdentityOptions(options, always != null && always);
            command.setNewColumn(newColumn);
            return command;
        }
        AlterSequence command = new AlterSequence(session, schema);
        command.setColumn(column, always);
        command.setOptions(options);
        return commandIfTableExists(schema, tableName, ifTableExists, command);
    }

    private Prepared parseAlterTableAlterColumnSet(Schema schema, String tableName, boolean ifTableExists,
            boolean ifExists, String columnName, Column column) {
        if (readIf("DATA")) {
            read("TYPE");
            return parseAlterTableAlterColumnDataType(schema, tableName, columnName, ifTableExists, ifExists);
        }
        AlterTableAlterColumn command = new AlterTableAlterColumn(
                session, schema);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setOldColumn(column);
        NullConstraintType nullConstraint = parseNotNullConstraint();
        switch (nullConstraint) {
        case NULL_IS_ALLOWED:
            command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
            break;
        case NULL_IS_NOT_ALLOWED:
            command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL);
            break;
        case NO_NULL_CONSTRAINT_FOUND:
            if (readIf(DEFAULT)) {
                if (readIf(ON)) {
                    read(NULL);
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT_ON_NULL);
                    command.setBooleanFlag(true);
                    break;
                }
                Expression defaultExpression = readExpression();
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT);
                command.setDefaultExpression(defaultExpression);
            } else if (readIf(ON)) {
                read("UPDATE");
                Expression onUpdateExpression = readExpression();
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_ON_UPDATE);
                command.setDefaultExpression(onUpdateExpression);
            } else if (readIf("INVISIBLE")) {
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY);
                command.setBooleanFlag(false);
            } else if (readIf("VISIBLE")) {
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY);
                command.setBooleanFlag(true);
            }
            break;
        default:
            throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                    "Internal Error - unhandled case: " + nullConstraint.name());
        }
        return command;
    }

    private Prepared parseAlterTableDrop(Schema schema, String tableName, boolean ifTableExists) {
        if (readIf(CONSTRAINT)) {
            boolean ifExists = readIfExists(false);
            String constraintName = readIdentifierWithSchema(schema.getName());
            ifExists = readIfExists(ifExists);
            checkSchema(schema);
            AlterTableDropConstraint command = new AlterTableDropConstraint(session, getSchema(), ifExists);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setConstraintName(constraintName);
            ConstraintActionType dropAction = parseCascadeOrRestrict();
            if (dropAction != null) {
                command.setDropAction(dropAction);
            }
            return command;
        } else if (readIf(PRIMARY)) {
            read(KEY);
            Table table = tableIfTableExists(schema, tableName, ifTableExists);
            if (table == null) {
                return new NoOperation(session);
            }
            Index idx = table.getPrimaryKey();
            DropIndex command = new DropIndex(session, schema);
            command.setIndexName(idx.getName());
            return command;
        } else if (database.getMode().alterTableExtensionsMySQL) {
            Prepared command = parseAlterTableDropCompatibility(schema, tableName, ifTableExists);
            if (command != null) {
                return command;
            }
        }
        readIf("COLUMN");
        boolean ifExists = readIfExists(false);
        ArrayList<Column> columnsToRemove = new ArrayList<>();
        Table table = tableIfTableExists(schema, tableName, ifTableExists);
        // For Oracle compatibility - open bracket required
        boolean openingBracketDetected = readIf(OPEN_PAREN);
        do {
            String columnName = readIdentifier();
            if (table != null) {
                Column column = table.getColumn(columnName, ifExists);
                if (column != null) {
                    columnsToRemove.add(column);
                }
            }
        } while (readIf(COMMA));
        if (openingBracketDetected) {
            // For Oracle compatibility - close bracket
            read(CLOSE_PAREN);
        }
        if (table == null || columnsToRemove.isEmpty()) {
            return new NoOperation(session);
        }
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        command.setType(CommandInterface.ALTER_TABLE_DROP_COLUMN);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setColumnsToRemove(columnsToRemove);
        return command;
    }

    private Prepared parseAlterTableDropCompatibility(Schema schema, String tableName, boolean ifTableExists) {
        if (readIf(FOREIGN)) {
            read(KEY);
            // For MariaDB
            boolean ifExists = readIfExists(false);
            String constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            AlterTableDropConstraint command = new AlterTableDropConstraint(session, getSchema(), ifExists);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setConstraintName(constraintName);
            return command;
        } else if (readIf("INDEX")) {
            // For MariaDB
            boolean ifExists = readIfExists(false);
            String indexOrConstraintName = readIdentifierWithSchema(schema.getName());
            if (schema.findIndex(session, indexOrConstraintName) != null) {
                DropIndex dropIndexCommand = new DropIndex(session, getSchema());
                dropIndexCommand.setIndexName(indexOrConstraintName);
                return commandIfTableExists(schema, tableName, ifTableExists, dropIndexCommand);
            } else {
                AlterTableDropConstraint dropCommand = new AlterTableDropConstraint(session, getSchema(), ifExists);
                dropCommand.setTableName(tableName);
                dropCommand.setIfTableExists(ifTableExists);
                dropCommand.setConstraintName(indexOrConstraintName);
                return dropCommand;
            }
        }
        return null;
    }

    private Prepared parseAlterTableRename(Schema schema, String tableName, boolean ifTableExists) {
        if (readIf("COLUMN")) {
            // PostgreSQL syntax
            String columnName = readIdentifier();
            read(TO);
            AlterTableRenameColumn command = new AlterTableRenameColumn(
                    session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setOldColumnName(columnName);
            command.setNewColumnName(readIdentifier());
            return command;
        } else if (readIf(CONSTRAINT)) {
            String constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            read(TO);
            AlterTableRenameConstraint command = new AlterTableRenameConstraint(session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setConstraintName(constraintName);
            command.setNewConstraintName(readIdentifier());
            return command;
        } else {
            read(TO);
            String newName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            AlterTableRename command = new AlterTableRename(session,
                    getSchema());
            command.setTableName(tableName);
            command.setNewTableName(newName);
            command.setIfTableExists(ifTableExists);
            command.setHidden(readIf("HIDDEN"));
            return command;
        }
    }

    private Prepared parseAlterTableSet(Schema schema, String tableName, boolean ifTableExists) {
        read("REFERENTIAL_INTEGRITY");
        int type = CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY;
        boolean value = readBooleanSetting();
        AlterTableSet command = new AlterTableSet(session,
                schema, type, value);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        if (readIf(CHECK)) {
            command.setCheckExisting(true);
        } else if (readIf("NOCHECK")) {
            command.setCheckExisting(false);
        }
        return command;
    }

    private Prepared parseAlterTableCompatibility(Schema schema, String tableName, boolean ifTableExists, Mode mode) {
        if (mode.alterTableExtensionsMySQL) {
            if (readIf("AUTO_INCREMENT")) {
                readIf(EQUAL);
                Expression restart = readExpression();
                Table table = tableIfTableExists(schema, tableName, ifTableExists);
                if (table == null) {
                    return new NoOperation(session);
                }
                Index idx = table.findPrimaryKey();
                if (idx != null) {
                    for (IndexColumn ic : idx.getIndexColumns()) {
                        Column column = ic.column;
                        if (column.isIdentity()) {
                            AlterSequence command = new AlterSequence(session, schema);
                            command.setColumn(column, null);
                            SequenceOptions options = new SequenceOptions();
                            options.setRestartValue(restart);
                            command.setOptions(options);
                            return command;
                        }
                    }
                }
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, "AUTO_INCREMENT PRIMARY KEY");
            } else if (readIf("CHANGE")) {
                readIf("COLUMN");
                String columnName = readIdentifier();
                String newColumnName = readIdentifier();
                Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists, false);
                boolean nullable = column == null ? true : column.isNullable();
                // new column type ignored. RENAME and MODIFY are
                // a single command in MySQL but two different commands in H2.
                parseColumnForTable(newColumnName, nullable);
                AlterTableRenameColumn command = new AlterTableRenameColumn(session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumnName(columnName);
                command.setNewColumnName(newColumnName);
                return command;
            } else if (readIf("CONVERT")) {
                readIf(TO);
                readIf("CHARACTER");
                readIf(SET);
                readMySQLCharset();

                if (readIf("COLLATE")) {
                    readMySQLCharset();
                }

                return new NoOperation(session);
            }
        }
        if (mode.alterTableModifyColumn && readIf("MODIFY")) {
            // MySQL compatibility (optional)
            readIf("COLUMN");
            // Oracle specifies (but will not require) an opening parenthesis
            boolean hasOpeningBracket = readIf(OPEN_PAREN);
            String columnName = readIdentifier();
            AlterTableAlterColumn command;
            NullConstraintType nullConstraint = parseNotNullConstraint();
            switch (nullConstraint) {
            case NULL_IS_ALLOWED:
            case NULL_IS_NOT_ALLOWED:
                command = new AlterTableAlterColumn(session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists, false);
                command.setOldColumn(column);
                if (nullConstraint == NullConstraintType.NULL_IS_ALLOWED) {
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
                } else {
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL);
                }
                break;
            case NO_NULL_CONSTRAINT_FOUND:
                command = parseAlterTableAlterColumnType(schema, tableName, columnName, ifTableExists, false,
                        mode.getEnum() != ModeEnum.MySQL);
                break;
            default:
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                        "Internal Error - unhandled case: " + nullConstraint.name());
            }
            if (hasOpeningBracket) {
                read(CLOSE_PAREN);
            }
            return command;
        }
        throw getSyntaxError();
    }

    private Table tableIfTableExists(Schema schema, String tableName, boolean ifTableExists) {
        Table table = schema.resolveTableOrView(session, tableName);
        if (table == null && !ifTableExists) {
            throw getTableOrViewNotFoundDbException(schema.getName(), tableName);
        }
        return table;
    }

    private Column columnIfTableExists(Schema schema, String tableName,
            String columnName, boolean ifTableExists, boolean ifExists) {
        Table table = tableIfTableExists(schema, tableName, ifTableExists);
        if (table == null) {
            return null;
        }
        return table.getColumn(columnName, ifExists);
    }

    private Prepared commandIfTableExists(Schema schema, String tableName,
            boolean ifTableExists, Prepared commandIfTableExists) {
        return tableIfTableExists(schema, tableName, ifTableExists) == null
            ? new NoOperation(session)
            : commandIfTableExists;
    }

    private AlterTableAlterColumn parseAlterTableAlterColumnType(Schema schema,
            String tableName, String columnName, boolean ifTableExists, boolean ifExists, boolean preserveNotNull) {
        Column oldColumn = columnIfTableExists(schema, tableName, columnName, ifTableExists, ifExists);
        Column newColumn = parseColumnForTable(columnName,
                !preserveNotNull || oldColumn == null || oldColumn.isNullable());
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        parseAlterColumnUsingIf(command);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE);
        command.setOldColumn(oldColumn);
        command.setNewColumn(newColumn);
        return command;
    }

    private AlterTableAlterColumn parseAlterTableAlterColumnDataType(Schema schema,
            String tableName, String columnName, boolean ifTableExists, boolean ifExists) {
        Column oldColumn = columnIfTableExists(schema, tableName, columnName, ifTableExists, ifExists);
        Column newColumn = parseColumnWithType(columnName);
        if (oldColumn != null) {
            if (!oldColumn.isNullable()) {
                newColumn.setNullable(false);
            }
            if (!oldColumn.getVisible()) {
                newColumn.setVisible(false);
            }
            Expression e = oldColumn.getDefaultExpression();
            if (e != null) {
                if (oldColumn.isGenerated()) {
                    newColumn.setGeneratedExpression(e);
                } else {
                    newColumn.setDefaultExpression(session, e);
                }
            }
            e = oldColumn.getOnUpdateExpression();
            if (e != null) {
                newColumn.setOnUpdateExpression(session, e);
            }
            Sequence s = oldColumn.getSequence();
            if (s != null) {
                newColumn.setIdentityOptions(new SequenceOptions(s, newColumn.getType()),
                        oldColumn.isGeneratedAlways());
            }
            String c = oldColumn.getComment();
            if (c != null) {
                newColumn.setComment(c);
            }
        }
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        parseAlterColumnUsingIf(command);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE);
        command.setOldColumn(oldColumn);
        command.setNewColumn(newColumn);
        return command;
    }

    private AlterTableAlterColumn parseAlterTableAddColumn(String tableName,
            Schema schema, boolean ifTableExists) {
        readIf("COLUMN");
        AlterTableAlterColumn command = new AlterTableAlterColumn(session,
                schema);
        command.setType(CommandInterface.ALTER_TABLE_ADD_COLUMN);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        if (readIf(OPEN_PAREN)) {
            command.setIfNotExists(false);
            do {
                parseTableColumnDefinition(command, schema, tableName, false);
            } while (readIfMore());
        } else {
            boolean ifNotExists = readIfNotExists();
            command.setIfNotExists(ifNotExists);
            parseTableColumnDefinition(command, schema, tableName, false);
            parseAlterColumnUsingIf(command);
        }
        if (readIf("BEFORE")) {
            command.setAddBefore(readIdentifier());
        } else if (readIf("AFTER")) {
            command.setAddAfter(readIdentifier());
        } else if (readIf("FIRST")) {
            command.setAddFirst();
        }
        return command;
    }

    private void parseAlterColumnUsingIf(AlterTableAlterColumn command) {
        if (readIf(USING)) {
            command.setUsingExpression(readExpression());
        }
    }

    private ConstraintActionType parseAction() {
        ConstraintActionType result = parseCascadeOrRestrict();
        if (result != null) {
            return result;
        }
        if (readIf("NO")) {
            read("ACTION");
            return ConstraintActionType.RESTRICT;
        }
        read(SET);
        if (readIf(NULL)) {
            return ConstraintActionType.SET_NULL;
        }
        read(DEFAULT);
        return ConstraintActionType.SET_DEFAULT;
    }

    private ConstraintActionType parseCascadeOrRestrict() {
        if (readIf("CASCADE")) {
            return ConstraintActionType.CASCADE;
        } else if (readIf("RESTRICT")) {
            return ConstraintActionType.RESTRICT;
        } else {
            return null;
        }
    }

    private DefineCommand parseAlterTableAddConstraintIf(String tableName, Schema schema, boolean ifTableExists) {
        String constraintName = null, comment = null;
        boolean ifNotExists = false;
        if (readIf(CONSTRAINT)) {
            ifNotExists = readIfNotExists();
            constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            comment = readCommentIf();
        }
        AlterTableAddConstraint command;
        switch (currentTokenType) {
        case PRIMARY:
            read();
            read(KEY);
            command = new AlterTableAddConstraint(session, schema,
                    CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY, ifNotExists);
            if (readIf("HASH")) {
                command.setPrimaryKeyHash(true);
            }
            read(OPEN_PAREN);
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            break;
        case UNIQUE:
            read();
            // MySQL compatibility
            boolean compatibility = database.getMode().indexDefinitionInCreateTable;
            if (compatibility) {
                if (!readIf(KEY)) {
                    readIf("INDEX");
                }
                if (!isToken(OPEN_PAREN)) {
                    constraintName = readIdentifier();
                }
            }
            read(OPEN_PAREN);
            command = new AlterTableAddConstraint(session, schema, CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE,
                    ifNotExists);
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            if (compatibility && readIf(USING)) {
                read("BTREE");
            }
            break;
        case FOREIGN:
            read();
            command = new AlterTableAddConstraint(session, schema,
                    CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL, ifNotExists);
            read(KEY);
            read(OPEN_PAREN);
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(schema.findIndex(session, indexName));
            }
            read("REFERENCES");
            parseReferences(command, schema, tableName);
            break;
        case CHECK:
            read();
            command = new AlterTableAddConstraint(session, schema, CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK,
                    ifNotExists);
            command.setCheckExpression(readExpression());
            break;
        default:
            if (constraintName == null) {
                Mode mode = database.getMode();
                if (mode.indexDefinitionInCreateTable) {
                    int start = lastParseIndex;
                    if (readIf(KEY) || readIf("INDEX")) {
                        // MySQL
                        // need to read ahead, as it could be a column name
                        if (DataType.getTypeByName(currentToken, mode) == null) {
                            CreateIndex createIndex = new CreateIndex(session, schema);
                            createIndex.setComment(comment);
                            createIndex.setTableName(tableName);
                            createIndex.setIfTableExists(ifTableExists);
                            if (!readIf(OPEN_PAREN)) {
                                createIndex.setIndexName(readIdentifier());
                                read(OPEN_PAREN);
                            }
                            createIndex.setIndexColumns(parseIndexColumnList());
                            // MySQL compatibility
                            if (readIf(USING)) {
                                read("BTREE");
                            }
                            return createIndex;
                        } else {
                            // known data type
                            reread(start);
                        }
                    }
                }
                return null;
            } else {
                if (expectedList != null) {
                    addMultipleExpected(PRIMARY, UNIQUE, FOREIGN, CHECK);
                }
                throw getSyntaxError();
            }
        }
        if (command.getType() != CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
            if (readIf("NOCHECK")) {
                command.setCheckExisting(false);
            } else {
                readIf(CHECK);
                command.setCheckExisting(true);
            }
        }
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setConstraintName(constraintName);
        command.setComment(comment);
        return command;
    }

    private void parseReferences(AlterTableAddConstraint command,
            Schema schema, String tableName) {
        if (readIf(OPEN_PAREN)) {
            command.setRefTableName(schema, tableName);
            command.setRefIndexColumns(parseIndexColumnList());
        } else {
            String refTableName = readIdentifierWithSchema(schema.getName());
            command.setRefTableName(getSchema(), refTableName);
            if (readIf(OPEN_PAREN)) {
                command.setRefIndexColumns(parseIndexColumnList());
            }
        }
        if (readIf("INDEX")) {
            String indexName = readIdentifierWithSchema();
            command.setRefIndex(getSchema().findIndex(session, indexName));
        }
        while (readIf(ON)) {
            if (readIf("DELETE")) {
                command.setDeleteAction(parseAction());
            } else {
                read("UPDATE");
                command.setUpdateAction(parseAction());
            }
        }
        if (readIf(NOT)) {
            read("DEFERRABLE");
        } else {
            readIf("DEFERRABLE");
        }
    }

    private CreateLinkedTable parseCreateLinkedTable(boolean temp,
            boolean globalTemp, boolean force) {
        read(TABLE);
        boolean ifNotExists = readIfNotExists();
        String tableName = readIdentifierWithSchema();
        CreateLinkedTable command = new CreateLinkedTable(session, getSchema());
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setForce(force);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        read(OPEN_PAREN);
        command.setDriver(readString());
        read(COMMA);
        command.setUrl(readString());
        read(COMMA);
        command.setUser(readString());
        read(COMMA);
        command.setPassword(readString());
        read(COMMA);
        String originalTable = readString();
        if (readIf(COMMA)) {
            command.setOriginalSchema(originalTable);
            originalTable = readString();
        }
        command.setOriginalTable(originalTable);
        read(CLOSE_PAREN);
        if (readIf("EMIT")) {
            read("UPDATES");
            command.setEmitUpdates(true);
        } else if (readIf("READONLY")) {
            command.setReadOnly(true);
        }
        if (readIf("FETCH_SIZE")) {
            command.setFetchSize(readNonNegativeInt());
        }
        if(readIf("AUTOCOMMIT")){
            if(readIf("ON")) {
                command.setAutoCommit(true);
            }
            else if(readIf("OFF")){
                command.setAutoCommit(false);
            }
        }
        return command;
    }

    private CreateTable parseCreateTable(boolean temp, boolean globalTemp,
            boolean persistIndexes) {
        boolean ifNotExists = readIfNotExists();
        String tableName = readIdentifierWithSchema();
        if (temp && globalTemp && equalsToken("SESSION", schemaName)) {
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
        if (readIf(OPEN_PAREN)) {
            if (!readIf(CLOSE_PAREN)) {
                do {
                    parseTableColumnDefinition(command, schema, tableName, true);
                } while (readIfMore());
            }
        }
        if (database.getMode().getEnum() == ModeEnum.MySQL) {
            parseCreateTableMySQLTableOptions(command);
        }
        if (readIf("ENGINE")) {
            command.setTableEngine(readIdentifier());
        }
        if (readIf(WITH)) {
            command.setTableEngineParams(readTableEngineParams());
        }
        if (temp) {
            if (readIf(ON)) {
                read("COMMIT");
                if (readIf("DROP")) {
                    command.setOnCommitDrop();
                } else if (readIf("DELETE")) {
                    read("ROWS");
                    command.setOnCommitTruncate();
                }
            } else if (readIf(NOT)) {
                if (readIf("PERSISTENT")) {
                    command.setPersistData(false);
                } else {
                    read("LOGGED");
                }
            }
            if (readIf("TRANSACTIONAL")) {
                command.setTransactional(true);
            }
        } else if (!persistIndexes && readIf(NOT)) {
            read("PERSISTENT");
            command.setPersistData(false);
        }
        if (readIf("HIDDEN")) {
            command.setHidden(true);
        }
        if (readIf(AS)) {
            readIf("SORTED");
            command.setQuery(parseQuery());
            if (readIf(WITH)) {
                command.setWithNoData(readIf("NO"));
                read("DATA");
            }
        }
        return command;
    }

    private void parseTableColumnDefinition(CommandWithColumns command, Schema schema, String tableName,
            boolean forCreateTable) {
        DefineCommand c = parseAlterTableAddConstraintIf(tableName, schema, false);
        if (c != null) {
            command.addConstraintCommand(c);
            return;
        }
        String columnName = readIdentifier();
        if (forCreateTable && (currentTokenType == COMMA || currentTokenType == CLOSE_PAREN)) {
            command.addColumn(new Column(columnName, TypeInfo.TYPE_UNKNOWN));
            return;
        }
        Column column = parseColumnForTable(columnName, true);
        if (column.hasIdentityOptions() && column.isPrimaryKey()) {
            command.addConstraintCommand(newPrimaryKeyConstraintCommand(session, schema, tableName, column));
        }
        command.addColumn(column);
        readColumnConstraints(command, schema, tableName, column);
    }

    /**
     * Create a new alter table command.
     *
     * @param session the session
     * @param schema the schema
     * @param tableName the table
     * @param column the column
     * @return the command
     */
    public static AlterTableAddConstraint newPrimaryKeyConstraintCommand(SessionLocal session, Schema schema,
            String tableName, Column column) {
        column.setPrimaryKey(false);
        AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema,
                CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY, false);
        pk.setTableName(tableName);
        pk.setIndexColumns(new IndexColumn[] { new IndexColumn(column.getName()) });
        return pk;
    }

    private void readColumnConstraints(CommandWithColumns command, Schema schema, String tableName, Column column) {
        String comment = column.getComment();
        boolean hasPrimaryKey = false, hasNotNull = false;
        NullConstraintType nullType;
        Mode mode = database.getMode();
        for (;;) {
            String constraintName;
            if (readIf(CONSTRAINT)) {
                constraintName = readIdentifier();
            } else if (comment == null && (comment = readCommentIf()) != null) {
                // Compatibility: COMMENT may be specified appear after some constraint
                column.setComment(comment);
                continue;
            } else {
                constraintName = null;
            }
            if (!hasPrimaryKey && readIf(PRIMARY)) {
                read(KEY);
                hasPrimaryKey = true;
                boolean hash = readIf("HASH");
                AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema,
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY, false);
                pk.setConstraintName(constraintName);
                pk.setPrimaryKeyHash(hash);
                pk.setTableName(tableName);
                pk.setIndexColumns(new IndexColumn[] { new IndexColumn(column.getName()) });
                command.addConstraintCommand(pk);
            } else if (readIf(UNIQUE)) {
                AlterTableAddConstraint unique = new AlterTableAddConstraint(session, schema,
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE, false);
                unique.setConstraintName(constraintName);
                unique.setIndexColumns(new IndexColumn[] { new IndexColumn(column.getName()) });
                unique.setTableName(tableName);
                command.addConstraintCommand(unique);
            } else if (!hasNotNull
                    && (nullType = parseNotNullConstraint()) != NullConstraintType.NO_NULL_CONSTRAINT_FOUND) {
                hasNotNull = true;
                if (nullType == NullConstraintType.NULL_IS_NOT_ALLOWED) {
                    column.setNullable(false);
                } else if (nullType == NullConstraintType.NULL_IS_ALLOWED) {
                    if (column.isIdentity()) {
                        throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                    }
                    column.setNullable(true);
                }
            } else if (readIf(CHECK)) {
                AlterTableAddConstraint check = new AlterTableAddConstraint(session, schema,
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK, false);
                check.setConstraintName(constraintName);
                check.setTableName(tableName);
                check.setCheckExpression(readExpression());
                command.addConstraintCommand(check);
            } else if (readIf("REFERENCES")) {
                AlterTableAddConstraint ref = new AlterTableAddConstraint(session, schema,
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL, false);
                ref.setConstraintName(constraintName);
                ref.setIndexColumns(new IndexColumn[] { new IndexColumn(column.getName()) });
                ref.setTableName(tableName);
                parseReferences(ref, schema, tableName);
                command.addConstraintCommand(ref);
            } else if (constraintName == null) {
                if (column.getIdentityOptions() != null || !parseCompatibilityIdentity(column, mode)) {
                    return;
                }
            } else {
                throw getSyntaxError();
            }
        }
    }

    private boolean parseCompatibilityIdentity(Column column, Mode mode) {
        if (mode.autoIncrementClause && readIf("AUTO_INCREMENT")) {
            parseCompatibilityIdentityOptions(column);
            return true;
        }
        if (mode.identityClause && readIf("IDENTITY")) {
            parseCompatibilityIdentityOptions(column);
            return true;
        }
        return false;
    }

    private void parseCreateTableMySQLTableOptions(CreateTable command) {
        boolean requireNext = false;
        for (;;) {
            if (readIf("AUTO_INCREMENT")) {
                readIf(EQUAL);
                Expression value = readExpression();
                set: {
                    AlterTableAddConstraint primaryKey = command.getPrimaryKey();
                    if (primaryKey != null) {
                        for (IndexColumn ic : primaryKey.getIndexColumns()) {
                            String columnName = ic.columnName;
                            for (Column column : command.getColumns()) {
                                if (database.equalsIdentifiers(column.getName(), columnName)) {
                                    SequenceOptions options = column.getIdentityOptions();
                                    if (options != null) {
                                        options.setStartValue(value);
                                        break set;
                                    }
                                }
                            }
                        }
                    }
                    throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, "AUTO_INCREMENT PRIMARY KEY");
                }
            } else if (readIf(DEFAULT)) {
                if (readIf("CHARACTER")) {
                    read(SET);
                } else {
                    readIf("CHARSET");
                    readIf("COLLATE");
                }
                readMySQLCharset();
            } else if (readIf("CHARACTER")) {
                read(SET);
                readMySQLCharset();
            } else if (readIf("COLLATE")) {
                readMySQLCharset();
            } else if (readIf("CHARSET")) {
                readMySQLCharset();
            } else if (readIf("COMMENT")) {
                readIf(EQUAL);
                command.setComment(readString());
            } else if (readIf("ENGINE")) {
                readIf(EQUAL);
                readIdentifier();
            } else if (readIf("ROW_FORMAT")) {
                readIf(EQUAL);
                readIdentifier();
            } else if (requireNext) {
                throw getSyntaxError();
            } else {
                break;
            }
            requireNext = readIf(COMMA);
        }
    }

    private void readMySQLCharset() {
        readIf(EQUAL);
        readIdentifier();
    }

    /**
     * Enumeration describing null constraints
     */
    private enum NullConstraintType {
        NULL_IS_ALLOWED, NULL_IS_NOT_ALLOWED, NO_NULL_CONSTRAINT_FOUND
    }

    private NullConstraintType parseNotNullConstraint(NullConstraintType nullConstraint) {
        if (nullConstraint == NullConstraintType.NO_NULL_CONSTRAINT_FOUND) {
            nullConstraint = parseNotNullConstraint();
        }
        return nullConstraint;
    }

    private NullConstraintType parseNotNullConstraint() {
        NullConstraintType nullConstraint;
        if (readIf(NOT)) {
            read(NULL);
            nullConstraint = NullConstraintType.NULL_IS_NOT_ALLOWED;
        } else if (readIf(NULL)) {
            nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
        } else {
            return NullConstraintType.NO_NULL_CONSTRAINT_FOUND;
        }
        if (database.getMode().getEnum() == ModeEnum.Oracle) {
            nullConstraint = parseNotNullCompatibility(nullConstraint);
        }
        return nullConstraint;
    }

    private NullConstraintType parseNotNullCompatibility(NullConstraintType nullConstraint) {
        if (readIf("ENABLE")) {
            if (!readIf("VALIDATE") && readIf("NOVALIDATE")) {
                // Turn off constraint, allow NULLs
                nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
            }
        } else if (readIf("DISABLE")) {
            // Turn off constraint, allow NULLs
            nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
            if (!readIf("VALIDATE")) {
                readIf("NOVALIDATE");
            }
        }
        return nullConstraint;
    }

    private CreateSynonym parseCreateSynonym(boolean orReplace) {
        boolean ifNotExists = readIfNotExists();
        String name = readIdentifierWithSchema();
        Schema synonymSchema = getSchema();
        read(FOR);
        String tableName = readIdentifierWithSchema();

        Schema targetSchema = getSchema();
        CreateSynonym command = new CreateSynonym(session, synonymSchema);
        command.setName(name);
        command.setSynonymFor(tableName);
        command.setSynonymForSchema(targetSchema);
        command.setComment(readCommentIf());
        command.setIfNotExists(ifNotExists);
        command.setOrReplace(orReplace);
        return command;
    }

    private static int getCompareType(int tokenType) {
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
        case SPATIAL_INTERSECTS:
            return Comparison.SPATIAL_INTERSECTS;
        default:
            return -1;
        }
    }

    /**
     * Add double quotes around an identifier if required.
     *
     * @param s the identifier
     * @param sqlFlags formatting flags
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String s, int sqlFlags) {
        if (s == null) {
            return "\"\"";
        }
        if ((sqlFlags & HasSQL.QUOTE_ONLY_WHEN_REQUIRED) != 0 && ParserUtil.isSimpleIdentifier(s, false, false)) {
            return s;
        }
        return StringUtils.quoteIdentifier(s);
    }

    public void setLiteralsChecked(boolean literalsChecked) {
        this.literalsChecked = literalsChecked;
    }

    public void setRightsChecked(boolean rightsChecked) {
        this.rightsChecked = rightsChecked;
    }

    public void setSuppliedParameterList(ArrayList<Parameter> suppliedParameterList) {
        this.suppliedParameterList = suppliedParameterList;
    }

    /**
     * Parse a SQL code snippet that represents an expression.
     *
     * @param sql the code snippet
     * @return the expression object
     */
    public Expression parseExpression(String sql) {
        parameters = Utils.newSmallArrayList();
        initialize(sql);
        read();
        return readExpression();
    }

    /**
     * Parse a SQL code snippet that represents an expression for a domain constraint.
     *
     * @param sql the code snippet
     * @return the expression object
     */
    public Expression parseDomainConstraintExpression(String sql) {
        parameters = Utils.newSmallArrayList();
        initialize(sql);
        read();
        try {
            parseDomainConstraint = true;
            return readExpression();
        } finally {
            parseDomainConstraint = false;
        }
    }

    /**
     * Parse a SQL code snippet that represents a table name.
     *
     * @param sql the code snippet
     * @return the table object
     */
    public Table parseTableName(String sql) {
        parameters = Utils.newSmallArrayList();
        initialize(sql);
        read();
        return readTableOrView();
    }

    /**
     * Parses a list of column names or numbers in parentheses.
     *
     * @param sql the source SQL
     * @param offset the initial offset
     * @return the array of column names ({@code String[]}) or numbers
     *         ({@code int[]})
     * @throws DbException on syntax error
     */
    public Object parseColumnList(String sql, int offset) {
        initialize(sql);
        parseIndex = offset;
        read();
        read(OPEN_PAREN);
        if (readIf(CLOSE_PAREN)) {
            return Utils.EMPTY_INT_ARRAY;
        }
        if (isIdentifier()) {
            ArrayList<String> list = Utils.newSmallArrayList();
            do {
                if (!isIdentifier()) {
                    throw getSyntaxError();
                }
                list.add(currentToken);
                read();
            } while (readIfMore());
            return list.toArray(new String[0]);
        } else if (currentTokenType == LITERAL) {
            ArrayList<Integer> list = Utils.newSmallArrayList();
            do {
                list.add(readInt());
            } while (readIfMore());
            int count = list.size();
            int[] array = new int[count];
            for (int i = 0; i < count; i++) {
                array[i] = list.get(i);
            }
            return array;
        } else {
            throw getSyntaxError();
        }
    }

    /**
     * Returns the last parse index.
     *
     * @return the last parse index
     */
    public int getLastParseIndex() {
        return lastParseIndex;
    }

    @Override
    public String toString() {
        return StringUtils.addAsterisk(sqlCommand, parseIndex);
    }
}
