/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

char convertBuffer[512];

Descriptor* Descriptor::cast(void* pointer) {
    if(pointer==0) {
        return 0;
    }
    Descriptor* desc=Descriptor::cast(pointer);
    if(desc->m_magic!=MAGIC_DESCRIPTOR) {
        return 0;
    }
    return desc;
}

Descriptor::Descriptor(Connection* c) {
    init();
    m_connection = c;
    m_type = DT_SHARED;
}

Descriptor::Descriptor() {
    init();
    m_type = DT_DEFAULT;
}

void Descriptor::init() {
    m_magic = MAGIC_DESCRIPTOR;    
    m_state = D_ACTIVE;
    m_arraySize = 0;
    m_statusPointer = 0;
    m_rowsProcessedPointer = 0;
    m_rowWiseBinding = false;
    m_rowSize = 0;    
    m_id = 0;
    m_count = 0;
    m_error = 0;
}

void Descriptor::setBindingType(bool rowWiseBinding, int rowSize) {
    m_rowWiseBinding = rowWiseBinding;
    m_rowSize = rowSize;
}   

Descriptor::~Descriptor() {
    if(m_magic==MAGIC_DESCRIPTOR) {
        m_magic=0;
    } else {
        trace("~Descriptor %d",m_magic);
        return;
    }
    for(int i=0;i<m_records.size();i++) {
        delete m_records[i];
        m_records[i]=0;
    }
}

void Descriptor::readData(int i, Socket* s) {
    m_records[i]->readData(s);
}

DescriptorRecord* DescriptorRecord::cast(void* pointer) {
    if(pointer==0) {
        return 0;
    }
    DescriptorRecord* rec=(DescriptorRecord*)pointer;
    if(rec->m_magic!=MAGIC_DESCRIPTOR_RECORD) {
        return 0;
    }
    return rec;
}

void DescriptorRecord::sendParameterValue(Socket* s) {
    int length = m_targetBufferLength;
    if(m_statusPointer != 0) {
        if(*m_statusPointer == SQL_NULL_DATA) {
            s->writeInt(0);
            trace("   write null");
            return;
        } else if(*m_statusPointer == SQL_NTS) {
            trace("   len=max");
            length = m_targetBufferLength;
        } else if(*m_statusPointer == SQL_DEFAULT_PARAM) {
            trace("   len=SQL_DEFAULT_PARAM TODO");
            length = m_targetBufferLength;
        } else if(*m_statusPointer == SQL_DATA_AT_EXEC) {
            trace("   len=SQL_DATA_AT_EXEC TODO");
            length = m_targetBufferLength;            
        } else {
            length = *m_statusPointer;
            trace("   len = %d", length);
        }
    }
    if(m_pointer == 0) {
        trace("   pointer==0 TODO");
        s->writeInt(0);
        return;
    }
    switch(m_cDataType) {
    case SQL_C_SHORT: {
        SQLSMALLINT i = *((SQLSMALLINT*)m_pointer);
        trace("   write smallInt %d", i);
        s->writeInt(SQL_INTEGER);
        s->writeInt(i);
        break;
    }
    case SQL_C_LONG: {
        SQLINTEGER i = *((SQLINTEGER*)m_pointer);
        trace("   write int %d", i);
        s->writeInt(SQL_INTEGER);
        s->writeInt(i);
        break;
    }
    case SQL_C_CHAR: {
        if(length < 0) {
            trace("   write String with buffer %d TODO", length);
            s->writeInt(0);
        } else {
            char buff[length+1];
            strncpy(buff, (char*)m_pointer, length);
            buff[length] = 0;
            trace("   write string %s", buff);
            s->writeInt(SQL_VARCHAR);
            s->writeString(buff);
        }
        break;
    }
    default:
        trace("  unsupported data type %d", m_cDataType);
    }
}

const char* DescriptorRecord::getPrefix() {
    return getSuffix();
}

const char* DescriptorRecord::getSuffix() {
    switch(m_sqlDataType) {
    case SQL_VARCHAR:
    case SQL_TYPE_DATE:
    case SQL_TYPE_TIME:
    case SQL_TYPE_TIMESTAMP:
        return ",";
    case SQL_DECIMAL:
    case SQL_NUMERIC:
    case SQL_BIT:
    case SQL_TINYINT:
    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_BIGINT:
    case SQL_REAL:
    case SQL_FLOAT:
    case SQL_DOUBLE:
    default:
        return "";
    }    
}

int DescriptorRecord::getLength() {
    switch(m_sqlDataType) {
    case SQL_VARCHAR:
        return 255;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
        return 100;
    case SQL_BIT:
        return 1;
    case SQL_TINYINT:
        return 3;
    case SQL_SMALLINT:
        return 5;
    case SQL_INTEGER:
        return 10;
    case SQL_BIGINT:
        return 20;
    case SQL_REAL:
        return 7;
    case SQL_FLOAT:
    case SQL_DOUBLE:
        return 15;
    case SQL_TYPE_DATE:
        return 10;
    case SQL_TYPE_TIME:
        return 8;
    case SQL_TYPE_TIMESTAMP:
        return 40;
    default:
        return 255;
    }
}

void DescriptorRecord::copyData(DescriptorRecord* ar) {
    if(m_wasNull) {
        ar->setNull();
    } else {
        switch(ar->m_cDataType) {
        case SQL_C_CHAR:
            trace("  SQL_CHAR / SQL_VARCHAR"); 
            returnString((SQLCHAR*)ar->m_pointer, ar->m_targetBufferLength, ar->m_statusPointer, getString());
            break;
        case SQL_C_SLONG:
        case SQL_C_ULONG:
            trace("  SQL_INTEGER"); 
            returnInt(ar->m_pointer, ar->m_statusPointer, getInt());
            break;
        case SQL_C_SSHORT:
            trace("  SQL_SMALLINT");         
            returnSmall((SQLSMALLINT*)ar->m_pointer, ar->m_statusPointer, getInt());
            break;
        default:
            trace("  ard not set");
        }        
    }
}

void DescriptorRecord::setNull() {
    if(m_statusPointer != 0) {
        *m_statusPointer = SQL_NULL_DATA;
    }
}

void DescriptorRecord::readData(Socket* s) {
    m_wasNull = false;
    switch(m_sqlDataType) {
    case 0: {
        trace("   read null");
        m_wasNull = true;
        m_dataInt = 0;
        break;
    }
    case SQL_SMALLINT: {
        trace("   read smallInt");
        m_wasNull = s->readBool();
        if(m_wasNull) {
            m_dataInt = 0;
        } else {
            m_dataInt = s->readInt();
        }
        break;
    }
    case SQL_INTEGER: {
        trace("   read int");
        m_wasNull = s->readBool();
        if(m_wasNull) {
            m_dataInt = 0;
        } else {
            m_dataInt = s->readInt();
        }
        break;
    }
    case SQL_VARCHAR: {
        m_dataString = s->readString();
        trace("   read string=%s", (char*)m_dataString.data());
        break;
    }
    default:
        trace(" unsupported data type %d", (char*)m_dataString.data());
    }
}

void DescriptorRecord::readMeta(Socket* s) {
    m_sqlDataType = s->readInt();
    m_tableName = s->readString();
    m_name = s->readString();
    m_columnName = m_name;
    m_precision = s->readInt();
    m_scale = s->readInt();
    m_displaySize = s->readInt();
    trace("  %s", m_name.data());
}

char* DescriptorRecord::getString() {
    switch(m_sqlDataType) {
    case SQL_VARCHAR:
        trace("  getString s=%s", (char*)m_dataString.data());
        return (char*)m_dataString.data();
    case SQL_SMALLINT: 
    case SQL_INTEGER: {
        itoa(m_dataInt, convertBuffer, 10);
        trace("  getString int %s", convertBuffer);
        return convertBuffer;
    }
    case 0:
        trace("  getString null");
        return 0;
    default:
        trace("unsupported type=%d", m_sqlDataType);
    }
    return "";
}

int DescriptorRecord::getInt() {
    switch(m_sqlDataType) {
    case SQL_VARCHAR:
        return atoi(m_dataString.data());
    case SQL_SMALLINT: 
    case SQL_INTEGER: {
        return m_dataInt;
    }
    case 0:
        return 0;
    default:
        trace("unsupported type=%d", m_sqlDataType);
        return 0;
    }
}

bool DescriptorRecord::hasFixedPrecisionScale() {
    switch(m_sqlDataType) {
    case SQL_VARCHAR:
    case SQL_INTEGER:
    case SQL_SMALLINT:
    case 0:
        return false;
    default:
        trace("unsupported type=%d", m_sqlDataType);
        return false;
    }
}

