/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

void setString(char* dest, int dest_len, SQLCHAR* source, SQLSMALLINT source_len) {
    if(dest==0 || dest_len==0) {
        return;
    }
    if(source==0) {
        *dest=0;
        return;
    }
    int len;
    if(source_len==SQL_NTS) {
        len=strlen((char*)source);
    } else {
        len=(int)source_len;
    }
    if(dest_len<len) {
        len=dest_len;
    }
    strncpy(dest, (char*)source, len);
    dest[len]=0;
}

void returnString(SQLPOINTER dest, SQLINTEGER dest_len, SQLINTEGER* dest_pt, const char* source) {
    if(dest_len==0) {
        return;
    }
    char* s = (char*)source;
    if(source==0) {
        s="";
    }
    int len=strlen(s);
    if(dest_len<len) {
        len=dest_len;
    }
    if(dest_pt!=0) {
        *dest_pt=len;
    }
    if(dest!=0) {
        strncpy((char*)dest, s, len);
        ((char*)dest)[len]=0;
    }
}

void returnString(SQLCHAR* dest, SQLSMALLINT dest_len, SQLSMALLINT* dest_pt, const char* source) {
    if(dest_len==0) {
        return;
    }
    char* s = (char*)source;
    if(source==0) {
        s="";
    }
    int len=strlen(s);
    if(dest_len<len) {
        len=dest_len;
    }
    if(dest_pt!=0) {
        *dest_pt=len;
    }
    if(dest!=0) {
        strncpy((char*)dest, s, len);
        dest[len]=0;
    }
}

void returnInt(SQLPOINTER InfoValue, SQLINTEGER* LengthPtr, SQLUINTEGER value) {
    if(LengthPtr!=0) {
        *LengthPtr=sizeof(SQLUINTEGER);
    }
    *((SQLUINTEGER*)InfoValue)=value;
}

void returnInt(SQLPOINTER NumericPtr, int value) {
    if(NumericPtr != 0) {
        *(SQLUINTEGER*)NumericPtr = value;
    }
}

void returnInt(SQLSMALLINT* pointer, int value) {
    if(pointer != 0) {
        *pointer = value;
    }
}

void returnInt(SQLPOINTER InfoValue, SQLSMALLINT* LengthPtr, SQLUINTEGER value) {
    if(LengthPtr!=0) {
        *LengthPtr=sizeof(SQLUINTEGER);
    }
    *((SQLUINTEGER*)InfoValue)=value;
}

void returnSmall(SQLPOINTER InfoValue, SQLINTEGER* LengthPtr, SQLUSMALLINT value) {
    if(LengthPtr!=0) {
        *LengthPtr=sizeof(SQLUSMALLINT);
    }
    *((SQLSMALLINT*)InfoValue)=value;
}

void returnSmall(SQLPOINTER InfoValue, SQLSMALLINT* LengthPtr, SQLUSMALLINT value) {
    if(LengthPtr!=0) {
        *LengthPtr=sizeof(SQLUSMALLINT);
    }
    *((SQLSMALLINT*)InfoValue)=value;
}

void returnPointer(SQLPOINTER pointer, void* value) {
    if(pointer==0) {
        return;
    }
    (*(void**)pointer)=value;
}

int getDefaultCType(int sqlType) {
    switch (sqlType) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
    case SQL_DECIMAL:
    case SQL_NUMERIC:
    case SQL_GUID:
        return SQL_C_CHAR;
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
        return SQL_C_WCHAR;
    case SQL_BIT:
        return SQL_C_BIT;
    case SQL_TINYINT:
        return SQL_C_TINYINT;
    case SQL_SMALLINT:
        return SQL_C_SHORT;
    case SQL_INTEGER:
        return SQL_C_LONG;
    case SQL_BIGINT:
        return SQL_C_SBIGINT;
    case SQL_REAL:
        return SQL_C_FLOAT;
    case SQL_FLOAT:
    case SQL_DOUBLE:
        return SQL_C_DOUBLE;
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        return SQL_C_BINARY;
    case SQL_TYPE_DATE:
        return SQL_C_DATE;
    case SQL_TYPE_TIME:
        return SQL_C_TIME;
    case SQL_TYPE_TIMESTAMP:
        return SQL_C_TIMESTAMP;
    default:
        trace("  unsupported translation from sqlType %d TODO", sqlType);
        return SQL_C_CHAR;
    }
}


