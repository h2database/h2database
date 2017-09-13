package org.h2.tools;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by vilyam on 13.09.17.
 */
public class OS implements SimpleRowSource {
    private static final String command = "ps -eoruser,pid,command";

    private BufferedReader input = null;

    public OS() {
        try {
            Process p = Runtime.getRuntime().exec(command);
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));

            input.readLine();// skip head
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ResultSet processes() {
        SimpleResultSet result = new SimpleResultSet(this);
        result.addColumn("user", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("pid", Types.INTEGER, Integer.MAX_VALUE, 0);
        result.addColumn("cmd", Types.VARCHAR, Integer.MAX_VALUE, 0);
        return result;
    }

    @Override
    public Object[] readRow() throws SQLException {
        if (input == null) {
            return null;
        }
        try {
            String line = input.readLine();
            if (line != null) {
                return line.split(" +");
            } else {
                return null;
            }
        } catch (IOException e) {
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, "Error processing").getSQLException();
        }
    }

    @Override
    public void close() {
        if (input != null) try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reset() throws SQLException {
        throw new SQLException("Method is not supported", "OS");
    }
}
