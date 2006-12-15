/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

Statement::Statement(Connection* c) {
    m_magic=MAGIC_STATEMENT;
    m_connection=c;
    m_appRow=&m_appRowDefault;
    m_impRow=&m_impRowDefault;
    m_appParam=&m_appParamDefault;
    m_impParam=&m_impParamDefault;
    m_columnCount = 0;
    m_updateCount = 0;
    m_resultSetId = -1;
    m_preparedId = 0;
    m_rowId = 0;
    m_hasResultSet = false;
    m_parameterCount = 0;    
    m_useBookmarks = false;
    m_error = 0;
}

Statement::~Statement() {
    if(m_magic==MAGIC_STATEMENT) {
        m_magic=0;
    } else {
        trace("~Statement %d",m_magic);
        return;
    }    
}

Statement* Statement::cast(void* pointer) {
    if(pointer==0) {
        return 0;
    }
    Statement* stat=(Statement*)pointer;
    if(stat->m_magic!=MAGIC_STATEMENT) {
        return 0;
    }
    return stat;
}

void Statement::processResultSet(Socket* s) {
    m_rowId = 0;
    m_updateCount = 0;
    m_state = S_EXECUTED;
    m_hasResultSet = true;
    m_resultSetId = s->readInt();
    m_columnCount = s->readInt();
    trace("  ResultSet id=%d cols=%d", m_resultSetId, m_columnCount);
    m_impRow->clearRecords();
    m_appRow->clearRecords();
    for(int i=0; i<m_columnCount; i++) {
        DescriptorRecord* rec = new DescriptorRecord(m_impRow);
        rec->readMeta(s);
        m_impRow->addRecord(rec);
        rec = new DescriptorRecord(m_appRow);
        m_appRow->addRecord(rec);        
    }
    trace("  imp=%d app=%d", m_impRow->getRecordCount(), m_appRow->getRecordCount());
}

void Statement::getMetaTables(char* catalog, char* schema, char* table, char* tabletypes) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('T');
    s->writeString(catalog);
    s->writeString(schema);
    s->writeString(table);
    s->writeString(tabletypes);
    processResultSet(s);
}

void Statement::getMetaBestRowIdentifier(char* catalog, char* schema, char* table, int scope, bool nullable) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('B');
    s->writeString(catalog);
    s->writeString(schema);
    s->writeString(table);
    s->writeInt(scope);
    s->writeBool(nullable);
    processResultSet(s);
}

void Statement::getMetaVersionColumns(char* catalog, char* schema, char* table) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('V');
    s->writeString(catalog);
    s->writeString(schema);
    s->writeString(table);
    processResultSet(s);
}

void Statement::getMetaIndexInfo(char* catalog, char* schema, char* table, bool unique, bool approximate) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('I');
    s->writeString(catalog);
    s->writeString(schema);
    s->writeString(table);
    s->writeBool(unique);
    s->writeBool(approximate);
    processResultSet(s);
}    

void Statement::getMetaColumns(char* catalog, char* schema, char* table, char* column) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('C');
    s->writeString(catalog);
    s->writeString(schema);
    s->writeString(table);
    s->writeString(column);
    processResultSet(s);    
}  

void Statement::getMetaTypeInfoAll() {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('D');
    s->writeByte('A');
    processResultSet(s);     
}

void Statement::getMetaTypeInfo(int sqlType) {
    Socket* s = m_connection->getSocket();
    s->writeByte('M');
    s->writeByte('D');
    s->writeByte('T');
    s->writeInt(sqlType);
    processResultSet(s);     
}

const char* Statement::getSQL() {
    return m_sql.data();
}

bool Statement::prepare(char* sql) {
    Socket* s = m_connection->getSocket();
    m_state = S_PREPARED;
    s->writeByte('P');
    s->writeString(sql);
    m_sql = sql;
    int result = s->readByte();
    if(result=='E') {
        m_parameterCount = 0;
        m_state = S_CLOSED;
        setError(E_42000);
        return false;
    } else if(result=='O') {    
        m_preparedId = s->readInt();
        m_parameterCount = s->readInt();
        m_impParam->clearRecords();
        m_appParam->clearRecords();
        trace("   prepared %d params=%d", m_preparedId, m_parameterCount);
    }
    return true;
}

void Statement::addParameter() {
    m_impParam->addRecord(new DescriptorRecord(m_impParam));
    m_appParam->addRecord(new DescriptorRecord(m_appParam));
}

bool Statement::executePrepared() {
    Socket* s = m_connection->getSocket();
    s->writeByte('Q');
    s->writeInt(m_preparedId);
    trace(" executePrepared %d", m_preparedId);
    Descriptor* desc = getAppParamDesc();
    for(int i=0; i<m_parameterCount; i++) {
        s->writeByte('1');
        s->writeInt(i);
        DescriptorRecord* rec = desc->getRecord(i);
        rec->sendParameterValue(s);
    }
    // end of parameters
    s->writeByte('0');
    
    int result = s->readByte();
    // TODO: this is copied source code from execute!
    m_state = S_EXECUTED;
    m_rowId = 0;
    m_resultSetId = 0;
    m_columnCount = 0;
    m_updateCount = 0;
    if(result=='E') {
        m_state = S_CLOSED;
        setError(E_42000);
        return false;
    } else if(result=='R') {
        processResultSet(s);
    } else if(result=='U') {
        m_state = S_EXECUTED;
        m_hasResultSet = false;
        m_updateCount = s->readInt();
    }
    return true;
}

bool Statement::execute(char* sql) {
    Socket* s = m_connection->getSocket();
    s->writeByte('E');
    s->writeString(sql);
    m_sql = sql;
    int result = s->readByte();
    m_state = S_EXECUTED;
    m_rowId = 0;
    m_resultSetId = 0;
    m_columnCount = 0;
    m_updateCount = 0;
    m_parameterCount = 0;
    if(result=='E') {
        m_state = S_CLOSED;
        // TODO set correct error
        setError(E_42000);
        return false;
    } else if(result=='R') {
        processResultSet(s);
    } else if(result=='U') {
        m_state = S_EXECUTED;
        m_hasResultSet = false;
        m_updateCount = s->readInt();
    } else if(result=='O') {
        m_preparedId = s->readInt();
        m_parameterCount = s->readInt();
        trace("   executeDirect prepared %d params=%d", m_preparedId, m_parameterCount);
        return executePrepared();
    }
    return true;
}

bool Statement::next() {
    if(!m_hasResultSet) {
        return false;
    }
    m_rowId++;
    Socket* s = m_connection->getSocket();
    s->writeByte('G');
    s->writeInt(m_resultSetId);
    int result = s->readByte();
    trace("  next %c", result);
    if(result=='E') {
        m_state = S_CLOSED;
        return false;
    } else if(result == '1') {
        for(int i=0; i<m_columnCount; i++) {
            m_impRow->readData(i, s);
            m_impRow->getRecord(i)->copyData(m_appRow->getRecord(i));
        }
        trace("  setStatus");
        m_impRow->setStatus(SQL_ROW_SUCCESS);
        trace("  setRowsProcessed");
        m_impRow->setRowsProcessed(1);
        return true;
    } else {
        m_impRow->setStatus(SQL_ROW_NOROW);
        m_impRow->setRowsProcessed(0);
        return false;
    }
    return m_resultSetId == 0;
} 

void Statement::closeCursor() {
    if(m_resultSetId >= 0) {
        Socket* s = m_connection->getSocket();
        s->writeByte('F');
        s->writeInt(m_resultSetId);
        m_resultSetId = -1;
    }
    m_state=S_CLOSED;
}

Descriptor* Statement::getAppRowDesc() {
    return m_appRow;
}

Descriptor* Statement::getImpRowDesc() {
    return m_impRow;
}

Descriptor* Statement::getAppParamDesc() {
    return m_appParam;
}

Descriptor* Statement::getImpParamDesc() {
    return m_impParam;
}

