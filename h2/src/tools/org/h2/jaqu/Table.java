/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.jaqu;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A class that implements the JaQu model mapping options.
 * <p>
 * You may implement the Table interface on your model object and optionally use
 * JQColumn annotations.<br>
 * <i>This imposes a compile-time and runtime-dependency on JaQu.</i>
 * <p>
 * <u>OR</u>
 * <p>
 * You may choose to use the JQTable and JQColumn annotations only.<br>
 * <i>This imposes a compile-time and runtime-dependency on this file only.</i>
 * <p>
 * <b>NOTE</b><br>
 * Classes that are annotated with JQTable <b>and</b> implement Table will NOT
 * call the define() method.
 * <p>
 * <b>Supported Data Types</b>
 * <table>
 * <tr>
 * <td>java.lang.String</td>
 * <td>VARCHAR (maxLength > 0) / TEXT (maxLength == 0)</td>
 * </tr>
 * <tr>
 * <td>java.lang.Boolean</td>
 * <td>BIT</td>
 * </tr>
 * <tr>
 * <td>java.lang.Byte</td>
 * <td>TINYINT</td>
 * </tr>
 * <tr>
 * <td>java.lang.Short</td>
 * <td>SMALLINT</td>
 * </tr>
 * <tr>
 * <td>java.lang.Integer</td>
 * <td>INT</td>
 * </tr>
 * <tr>
 * <td>java.lang.Long</td>
 * <td>BIGINT</td>
 * </tr>
 * <tr>
 * <td>java.lang.Float</td>
 * <td>REAL</td>
 * </tr>
 * <tr>
 * <td>java.lang.Double</td>
 * <td>DOUBLE</td>
 * </tr>
 * <tr>
 * <td>java.math.BigDecimal</td>
 * <td>DECIMAL</td>
 * </tr>
 * <tr>
 * <td>java.util.Date</td>
 * <td>TIMESTAMP</td>
 * </tr>
 * <tr>
 * <td>java.sql.Date</td>
 * <td>DATE</td>
 * </tr>
 * <tr>
 * <td>java.sql.Time</td>
 * <td>TIME</td>
 * </tr>
 * <tr>
 * <td>java.sql.Timestamp</td>
 * <td>TIMESTAMP</td>
 * </tr>
 * </table>
 * <p>
 * <b>Unsupported Data Types</b>
 * <ul>
 * <li>Binary types (BLOB, etc)
 * <li>Custom types
 * </ul>
 * <p>
 * <b>Table and Field Mapping</b>
 * <p>
 * By default, the mapped table name is the class name and the <i>public</i>
 * fields are reflectively mapped, by their name, to columns.
 * <p>
 * As an alternative, you may specify both the table and column definition by
 * annotations.
 * <p>
 * <b>Table Interface</b>
 * <p>
 * You may set additional parameters such as table name, primary key, and
 * indexes in the <i>define()</i> method.
 * <p>
 * <b>Annotations</b>
 * <p>
 * You may use the annotations with or without implementing the Table interface.
 * <br>
 * The annotations allow you to decouple your model completely from JaQu other
 * than this file.
 * <p>
 * <b>Automatic Model Generation</b>
 * <p>
 * You may automatically generate model classes as strings with the <i>Db</i>
 * and <i>DbInspector</i> objects.
 * 
 * <pre>
 * Db db = Db.open(&quot;jdbc:h2:mem:&quot;, &quot;sa&quot;, &quot;sa&quot;);
 * DbInspector inspector = new DbInspector(db);
 * List&lt;String&gt; models = inspector.generateModel(schema, table, packageName,
 * 									annotateSchema, trimStrings)
 * </pre>
 * 
 * OR you may use the <i>GenerateModels</i> tool to generate and save your
 * classes to the filesystem.
 * 
 * <pre>
 * java -cp h2jaqu.jar org.h2.jaqu.util.GenerateModels 
 *      -url &quot;jdbc:h2:mem:&quot;
 *      -user sa -password sa -schema schemaName -table tableName
 *      -package packageName -folder destination
 *      -annotateSchema false -trimStrings true
 * </pre>
 * 
 * <b>Model Validation</b>
 * <p>
 * You may validate your model class with <i>DbInspector</i> object.<br>
 * The DbInspector will report ERRORS, WARNINGS, and SUGGESTIONS to help you.
 * 
 * <pre>
 * Db db = Db.open(&quot;jdbc:h2:mem:&quot;, &quot;sa&quot;, &quot;sa&quot;);
 * DbInspector inspector = new DbInspector(db);
 * List&lt;Validation&gt; remarks = inspector.validateModel(new MyModel(), throwOnError);
 * for (Validation remark : remarks)
 *     System.out.println(remark);
 * </pre>
 */
public interface Table {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface JQDatabase {
        /**
         * If <b>version</b> is set to a <i>non-zero</i> value, JaQu will
         * maintain a "_jq_versions" table within your database. The
         * <i>version</i> number will be used to call to a registered
         * <i>DbUpgrader</i> implementation to perform relevant ALTERs or
         * whatever.
         * <p>
         * <b>Default: <i>0</i></b>
         * <p>
         * <b>NOTE:</b><br>
         * You must specify a <i>DbUpgrader</i> on your <i>Db</i> object to
         * use this parameter.
         */
        int version() default 0;
    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface JQSchema {
        /**
         * <b>schema</b> may be optionally specified. If it is not specified the
         * schema will be ignored.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String name() default "";
    }
    
    /**
     * Enumeration defining the 4 index types.
     *
     */
    public static enum IndexType {
        STANDARD, UNIQUE, HASH, UNIQUE_HASH;
    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface JQIndex {
        /**
         * <b>standard</b> indexes may be optionally specified. If not specified,
         * these values will be ignored.
         * <ul>
         * <li>standard = "id, name"
         * <li>standard = "id name"
         * <li>standard = { "id name", "date" }
         * </ul>
         * Standard indexes may still be added in the <i>define()</i> method if
         * the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String[] standard() default {};

        /**
         * <b>unique</b> indexes may be optionally specified. If not specified,
         * these values will be ignored.
         * <ul>
         * <li>unique = "id, name"
         * <li>unique = "id name"
         * <li>unique = { "id name", "date" }
         * </ul>
         * Unique indexes may still be added in the <i>define()</i> method if
         * the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String[] unique() default {};
   
        /**
         * <b>hash</b> indexes may be optionally specified. If not specified,
         * these values will be ignored.
         * <ul>
         * <li>hash = "name"
         * <li>hash = { "name", "date" }
         * </ul>
         * Hash indexes may still be added in the <i>define()</i> method if
         * the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String[] hash() default {};
   
        /**
         * <b>uniqueHash</b> indexes may be optionally specified. If not specified,
         * these values will be ignored.
         * <ul>
         * <li>uniqueHash = "id"
         * <li>uniqueHash = "name"
         * <li>uniqueHash = { "id", "name" }
         * </ul>
         * UniqueHash indexes may still be added in the <i>define()</i> method if
         * the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String[] uniqueHash() default {};
    }

    /**
     * Annotation to define a table.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface JQTable {

        /**
         * <b>name</b> may be optionally specified. If it is not specified the
         * class name will be used as the table name.
         * <p>
         * The table name may still be overridden in the <i>define()</i> method
         * if the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String name() default "";

        /**
         * <b>primaryKey</b> may be optionally specified. If it is not
         * specified, then no primary key will be set by the JQTable annotation.
         * You may specify a composite primary key.
         * <ul>
         * <li>primaryKey = "id, name"
         * <li>primaryKey = "id name"
         * </ul>
         * The primaryKey may still be overridden in the <i>define()</i> method
         * if the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>Unspecified</i></b>
         */
        String primaryKey() default "";

        /**
         * <b>inheritColumns</b> allows this model class to inherit columns from
         * its super class. Any JQTable annotation present on the super class is
         * ignored.<br>
         * <p>
         * <b>Default: <i>false</i></b>
         */
        boolean inheritColumns() default false;

        /**
         * <b>createIfRequired</b> allows user to control whether or not
         * JaQu tries to create the table and indexes.
         * <p>
         * <b>Default: <i>true</i></b>
         */
        boolean createIfRequired() default true;

        /**
         * <b>strictTypeMapping</b> allows user to specify that only supported
         * types are mapped.<br>
         * If set <i>true</i>, unsupported mapped types will throw a
         * RuntimeException.<br>
         * If set <i>false</i>, unsupported mapped types will default to
         * VARCHAR.
         * <p>
         * <b>Default: <i>true</i></b>
         */
        boolean strictTypeMapping() default true;

        /**
         * <b>annotationsOnly</b> controls reflective field mapping on your
         * model object. If set <i>true</i>, only fields that are explicitly
         * annotated as JQColumn are mapped.
         * <p>
         * <b>Default: <i>true</i></b>
         */
        boolean annotationsOnly() default true;

        /**
         * If <b>memoryTable</b> is set <i>true</i>, this table is created as a
         * memory table where data is persistent, but index data is kept in main
         * memory.<br>
         * The JDBC Connection class is verified before applying this property
         * in the CREATE phase.
         * <p>
         * <b>Default: <i>false</i></b>
         * <p>
         * <u>Valid only for H2 databases.</u>
         */
        boolean memoryTable() default false;

        /**
         * If <b>version</b> is set to a <i>non-zero</i> value, JaQu will
         * maintain a "_jq_versions" table within your database. The
         * <i>version</i> number will be used to call to a registered
         * <i>DbUpgrader</i> implementation to perform relevant ALTERs or
         * whatever.
         * <p>
         * <b>Default: <i>0</i></b>
         * <p>
         * <bNOTE:</b><br>
         * You must specify a <i>DbUpgrader</i> on your <i>Db</i> object to
         * use this parameter.
         */
        int version() default 0;
    }

    /**
     * Annotation to define a Column. Annotated fields may have any Scope with
     * the understanding that under some circumstances, the JVM may raise a
     * SecurityException.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface JQColumn {

        /**
         * If <b>name</b> is not specified the instance variable field name will
         * be used as the column name.
         * <p>
         * <b>Default: <i>reflective field name mapping</i></b>
         */
        String name() default "";

        /**
         * If <b>primaryKey</b> is true, this column will be the PrimaryKey.
         * <p>
         * <b>Default: <i>false</i></b>
         */
        boolean primaryKey() default false;

        /**
         * If <b>autoIncrement</b> is true, the column will be created with a
         * sequence as the default value.
         * <p>
         * <b>Default: <i>false</i></b>
         */
        boolean autoIncrement() default false;

        /**
         * If <b>maxLength</b> > 0 it is used during the CREATE TABLE phase. It
         * may also be optionally used to prevent database exceptions on INSERT
         * and UPDATE statements (see <i>trimString</i>).
         * <p>
         * Any maxLength set in <i>define()</i> may override this annotation
         * setting if the model class is not annotated with JQTable.
         * <p>
         * <b>Default: <i>0</i></b>
         */
        int maxLength() default 0;

        /**
         * If <b>trimString</b> is <i>true</i> JaQu will automatically trim the
         * string if it exceeds <b>maxLength</b>.
         * <p>
         * e.g. stringValue = stringValue.substring(0, maxLength)
         * <p>
         * <b>Default: <i>false</i></b>
         */
        boolean trimString() default false;

        /**
         * If <b>allowNull</b> is <i>false</i> then JaQu will set
         * the column NOT NULL during the CREATE TABLE phase.
         * <p>
         * <b>Default: <i>false</i></b>
         */
        boolean allowNull() default false;

        /**
         * <b>defaultValue</b> is the value assigned to the column during the
         * CREATE TABLE phase.
         * <p>
         * To set <b>null</b>, defaultValue="" (default)
         * <p>
         * This field could contain a literal <u>single-quoted value</u>.<br>
         * Or a function call.<br>
         * Empty strings will be considered NULL.
         * <ul>
         * <li>defaultValue="" (null)
         * <li>defaultValue="CURRENT_TIMESTAMP" (H2 current_timestamp())
         * <li>defaultValue="''" (default empty string)
         * <li>defaultValue="'0'" (default number)
         * <li>defaultValue="'1970-01-01 00:00:01'" (default date)
         * </ul>
         * if (
         * <ul>
         * <li>defaultValue is properly specified
         * <li>AND <i>autoIncrement</i> == false
         * <li>AND <i>primaryKey</i> == false
         * </ul>
         * )<br>
         * then this value will be included in the "DEFAULT ..." phrase of a
         * column during the CREATE TABLE process.
         * <p>
         * <b>Default: <i>unspecified, null</i></b>
         */
        String defaultValue() default "";
    }

    /**
     * This method is called to let the table define the primary key, indexes,
     * and the table name.
     */
    void define();
}
