/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * @author aschoerk
 */
public class TestIgnoreCatalogs extends TestDb {
    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        canCommentOn();
        canUseDefaultSchema();
        canYetIdentifyWrongCatalogName();
        canUseSettingInUrl();
        canUseSetterSyntax();
        canCatalogNameEqualSchemaName();
        canUseCatalogAtIndexName();
        canCommentOn();
        canAllCombined();
        doesNotAcceptEmptySchemaWhenNotMSSQL();
    }

    private void doesNotAcceptEmptySchemaWhenNotMSSQL() throws SQLException {
        try (Connection conn = getConnection("ignoreCatalogs;IGNORE_CATALOGS=TRUE")) {
            try (Statement stat = conn.createStatement()) {
                prepareDbAndSetDefaultSchema(stat);
                stat.execute("set schema dbo");
                stat.execute("create table catalog1.dbo.test(id int primary key, name varchar(255))");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on table catalog1..test is 'table comment3'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "create table catalog1..test2(id int primary key, "
                        + "name varchar(255))");
                stat.execute("comment on table catalog1.dbo.test is 'table comment1'");
                stat.execute("insert into test values(1, 'Hello')");
                stat.execute("insert into cat.dbo.test values(2, 'Hello2')");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column catalog1...test.id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column catalog1..test..id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column ..test..id is 'id comment1'");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canCommentOn() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;IGNORE_CATALOGS=TRUE;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDbAndSetDefaultSchema(stat);
                stat.execute("create table catalog1.dbo.test(id int primary key, name varchar(255))");
                stat.execute("comment on table catalog1.dbo.test is 'table comment1'");
                stat.execute("comment on table dbo.test is 'table comment2'");
                stat.execute("comment on table catalog1..test is 'table comment3'");
                stat.execute("comment on table test is 'table comment4'");
                stat.execute("comment on column catalog1..test.id is 'id comment1'");
                stat.execute("comment on column catalog1.dbo.test.id is 'id comment1'");
                stat.execute("comment on column dbo.test.id is 'id comment1'");
                stat.execute("comment on column test.id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column catalog1...id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column catalog1...test.id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column catalog1..test..id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column ..test..id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column test..id is 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column .PUBLIC.TEST.ID 'id comment1'");
                assertThrows(ErrorCode.SYNTAX_ERROR_2, stat, "comment on column .TEST.ID 'id comment1'");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canUseDefaultSchema() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;IGNORE_CATALOGS=TRUE;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDbAndSetDefaultSchema(stat);
                stat.execute("create table catalog1..test(id int primary key, name varchar(255))");

                stat.execute("create table test2(id int primary key, name varchar(255))");
                // expect table already exists
                assertThrows(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, stat,
                        "create table catalog2.dbo.test(id int primary key, name varchar(255))");
                stat.execute("insert into test values(1, 'Hello')");
                stat.execute("insert into test2 values(1, 'Hello')");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canUseSettingInUrl() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;IGNORE_CATALOGS=TRUE;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDb(stat);
                stat.execute("create table catalog1.dbo.test(id int primary key, name varchar(255))");
                // expect table already exists
                assertThrows(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, stat,
                        "create table catalog2.dbo.test(id int primary key, name varchar(255))");
                stat.execute("insert into dbo.test values(1, 'Hello')");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }

    }

    private void canUseSetterSyntax() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDb(stat);
                stat.execute("set IGNORE_CATALOGS=TRUE");
                stat.execute("create table catalog1.dbo.test(id int primary key, name varchar(255))");
                // expect table already exists
                assertThrows(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, stat,
                        "create table catalog2.dbo.test(id int primary key, name varchar(255))");
                stat.execute("insert into dbo.test values(1, 'Hello')");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canCatalogNameEqualSchemaName() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDb(stat);
                stat.execute("set IGNORE_CATALOGS=TRUE");
                stat.execute("create table dbo.dbo.test(id int primary key, name varchar(255))");
                // expect object already exists
                assertThrows(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, stat,
                        "create table catalog2.dbo.test(id int primary key, name varchar(255))");
                stat.execute("insert into dbo.test values(1, 'Hello')");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canYetIdentifyWrongCatalogName() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDb(stat);
                // works, since catalog name equals database name
                stat.execute("create table ignoreCatalogs.dbo.test(id int primary key, name varchar(255))");
                // schema test_x not found error
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "create table test_x.dbo.test(id int primary key, name varchar(255))");
                assertThrows(ErrorCode.DATABASE_NOT_FOUND_1, stat, "comment on column db..test.id is 'id'");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canUseCatalogAtIndexName() throws Exception {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDb(stat);
                stat.execute("set IGNORE_CATALOGS=TRUE");
                stat.execute("create table dbo.dbo.test(id int primary key, name varchar(255))");
                stat.execute("create index i on dbo.dbo.test(id,name)");
                stat.execute("create index dbo.i2 on dbo.dbo.test(id,name)");
                stat.execute("create index catalog.dbo.i3 on dbo.dbo.test(id,name)");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "create index dboNotExistent.i4 on dbo.dbo.test(id,name)");
                // expect object already exists
                stat.execute("insert into dbo.test values(1, 'Hello')");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private void canAllCombined() throws SQLException {
        try (Connection conn = getConnection("ignoreCatalogs;MODE=MSSQLSERVER;IGNORE_CATALOGS=TRUE;")) {
            try (Statement stat = conn.createStatement()) {
                prepareDbAndSetDefaultSchema(stat);
                stat.execute("create table dbo.test(id int primary key, name varchar(255))");
                stat.execute("create table catalog1.dbo.test2(id int primary key, name varchar(255))");
                stat.execute("insert into dbo.test values(1, 'Hello')");
                stat.execute("insert into dbo.test2 values(1, 'Hello2')");
                stat.execute("set ignore_catalogs=false");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "insert into catalog1.dbo.test2 values(2, 'Hello2')");
                stat.execute("set ignore_catalogs=true");
                assertResult("1", stat, "select * from test");
                assertResult("1", stat, "select * from test2");
                stat.execute("alter table xxx.dbo.test add column (a varchar(200))");
                stat.execute("alter table xxx..test add column (b varchar(200))");
                stat.execute("alter table test add column (c varchar(200))");
                stat.execute("drop table xxx.dbo.test");
                stat.execute("drop table catalog1.dbo.test2");
                stat.execute("drop table if exists xxx.dbo.test");
                stat.execute("drop table if exists catalog1.dbo.test2");
                stat.execute("set ignore_catalogs=false");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "alter table xxx.dbo.test add column (a varchar(200))");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "alter table xxx..test add column (b varchar(200))");
                assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat,
                        "alter table test add column (c varchar(200))");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "drop table if exists xxx.dbo.test");
                assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, stat,
                        "drop table if exists xxx2..test");
                assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat, "drop table test");
            }
        } finally {
            deleteDb("ignoreCatalogs");
        }
    }

    private static void prepareDb(Statement stat) throws SQLException {
        stat.execute("drop all objects");
        stat.execute("create schema dbo");
    }

    private static void prepareDbAndSetDefaultSchema(Statement stat) throws SQLException {
        prepareDb(stat);
        stat.execute("set schema dbo");
    }

}
