package org.h2.test.lostalias;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

public class LostAliasTest extends TestBase {

    public static void main(final String[] args) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        shouldNotLostAlias();
    }

    private void shouldNotLostAlias() throws SQLException {
        DeleteDbFiles.execute(getBaseDir(), "lostalias", true);
        Connection conn = getConnection("lostalias");

        Statement stat = conn.createStatement();

        stat.execute("create table entry(id int, region int, author int)");
        stat.execute("create table meta(id int, type varchar(255), value varchar(255))");
        stat.execute("create table person(id int, name varchar(255))");
        stat.execute("insert into person(id, name) values(3, 'edgar')");
        stat.execute("insert into meta(id, type, value) values(2, 'Region', 'LATAM')");
        stat.execute("insert into entry(id, region, author) values(1, 2, 3)");

        ResultSet rs = stat.executeQuery("select entry.*, region.*, author.* from entry entry join meta region on entry.region=region.id join person author on entry.author=author.id");
        while (rs.next()) {
            assertEquals(1, rs.getInt("entry.id"));
            assertEquals(2, rs.getInt("region.id"));
            assertEquals("Region", rs.getString("region.type"));
            assertEquals("LATAM", rs.getString("region.value"));
            assertEquals(3, rs.getInt("author.id"));
            assertEquals("edgar", rs.getString("author.name"));
        }
        rs.close();
        stat.close();
        conn.close();
    }
}
