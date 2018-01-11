package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
/**
 * Base class for common table expression tests
 */
public abstract class AbstractBaseForCommonTableExpressions extends TestBase {

    protected void testRepeatedQueryWithSetup(int maxRetries, String[] expectedRowData, String[] expectedColumnNames, int expectedNumbeOfRows, String setupSQL,
            String withQuery, int closeAndReopenDatabaseConnectionOnIteration, String[] expectedColumnTypes) throws SQLException {
            
                deleteDb("commonTableExpressionQueries");
                Connection conn = getConnection("commonTableExpressionQueries");
                PreparedStatement prep;
                ResultSet rs;
            
                
                for (int queryRunTries = 1; queryRunTries <= maxRetries; queryRunTries++) {
            
                    Statement stat = conn.createStatement();
                    stat.execute(setupSQL);
                    stat.close();
            
                    // close and re-open connection for one iteration to make sure the query work between connections
                    if (queryRunTries == closeAndReopenDatabaseConnectionOnIteration) {
                        conn.close();
                        
                        conn = getConnection("commonTableExpressionQueries");
                    }
                    prep = conn.prepareStatement(withQuery);
            
                    rs = prep.executeQuery();
                    for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++) {
            
                        assertTrue(rs.getMetaData().getColumnLabel(columnIndex) != null);
                        assertEquals(expectedColumnNames[columnIndex - 1], rs.getMetaData().getColumnLabel(columnIndex));
                        assertEquals("wrongly type column "+rs.getMetaData().getColumnLabel(columnIndex)+" on iteration#"+queryRunTries, 
                                expectedColumnTypes[columnIndex - 1], rs.getMetaData().getColumnTypeName(columnIndex));
                    }
            
                    int rowNdx = 0;
                    while (rs.next()) {
                        StringBuffer buf = new StringBuffer();
                        for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++) {
                            buf.append("|"+rs.getString(columnIndex));
                        }
                        assertEquals(expectedRowData[rowNdx], buf.toString());
                        rowNdx++;
                    }
            
                    assertEquals(expectedNumbeOfRows, rowNdx);
            
                    rs.close();
                    prep.close();
                }
            
                conn.close();
                deleteDb("commonTableExpressionQueries");
            
            }

}
