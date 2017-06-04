package org.h2.message;

import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcSQLException;

public class DbNotRecursiveException extends DbException {

	private static final long serialVersionUID = -5941745175474148318L;
	
    public static DbNotRecursiveException get(int errorCode, String p1) {
        return get(errorCode, new String[] { p1 });
    }
    public static DbNotRecursiveException get(int errorCode, String... params) {
        return new DbNotRecursiveException(getJdbcSQLException(errorCode, null, params));
    }    
    private static JdbcSQLException getJdbcSQLException(int errorCode,
            Throwable cause, String... params) {
        String sqlstate = ErrorCode.getState(errorCode);
        String message = translate(sqlstate, params);
        return new JdbcSQLException(message, null, sqlstate, errorCode, cause, null);
    }
    private DbNotRecursiveException(SQLException e) {
        super(e);
    }

}
