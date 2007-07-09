package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Session;

public class DeallocateProcedure extends DefineCommand {

    private String procedureName;
    
    public DeallocateProcedure(Session session) {
        super(session);
    }
    
    public int update() throws SQLException {
        session.removeProcedure(procedureName);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

}
