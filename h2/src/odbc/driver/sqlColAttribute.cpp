/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN SQLColAttribute (
     SQLHSTMT StatementHandle,
     SQLUSMALLINT ColumnNumber,
     SQLUSMALLINT FieldIdentifier,
     SQLPOINTER CharacterAttributePtr,
     SQLSMALLINT BufferLength,
     SQLSMALLINT* StringLengthPtr,
     SQLPOINTER NumericAttributePtr) {
    trace("SQLColAttribute col=%d", ColumnNumber);
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);        
    Descriptor* desc = stat->getImpRowDesc();    
    ColumnNumber--;
    if(ColumnNumber > stat->getColumnCount()) {
        trace(" columnCount=%d", stat->getColumnCount());
        return SQL_ERROR;
    }
    DescriptorRecord* rec = desc->getRecord(ColumnNumber);
    switch(FieldIdentifier) {
    case SQL_DESC_AUTO_UNIQUE_VALUE:
        trace(" SQL_DESC_AUTO_UNIQUE_VALUE");
        returnInt(NumericAttributePtr, SQL_FALSE);
        break;
    case SQL_DESC_BASE_COLUMN_NAME:
        trace(" SQL_DESC_BASE_COLUMN_NAME");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "");
        break;
    case SQL_DESC_BASE_TABLE_NAME:
        trace(" SQL_DESC_BASE_TABLE_NAME");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "");
        break;
    case SQL_DESC_CASE_SENSITIVE:
        trace(" SQL_DESC_CASE_SENSITIVE");
        returnInt(NumericAttributePtr, SQL_TRUE);
        break;
    case SQL_DESC_CATALOG_NAME:
        trace(" SQL_DESC_CATALOG_NAME");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "");
        break;
    case SQL_DESC_CONCISE_TYPE:
        trace(" SQL_DESC_CONCISE_TYPE");
        returnInt(NumericAttributePtr, rec->getSqlDataType());
        break;
    case SQL_DESC_COUNT:
        trace(" SQL_DESC_COUNT");
        returnInt(NumericAttributePtr, stat->getColumnCount());
        break;
    case SQL_DESC_DISPLAY_SIZE:
        trace(" SQL_DESC_DISPLAY_SIZE");
        returnInt(NumericAttributePtr, rec->getDisplaySize());
        break;
    case SQL_DESC_FIXED_PREC_SCALE:
        trace(" SQL_DESC_FIXED_PREC_SCALE");
        returnInt(NumericAttributePtr, rec->hasFixedPrecisionScale() ? SQL_TRUE : SQL_FALSE);
        break;
    case SQL_DESC_LABEL:
        trace(" SQL_DESC_LABEL");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, rec->getColumnName().data());
        break;
    case SQL_DESC_LENGTH:
        trace(" SQL_DESC_LENGTH =%d", rec->getLength());
        returnInt(NumericAttributePtr, rec->getLength());
        break;
    case SQL_DESC_LITERAL_PREFIX:
        trace(" SQL_DESC_LITERAL_PREFIX %s", rec->getPrefix());
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, rec->getPrefix());
        break;
    case SQL_DESC_LITERAL_SUFFIX:
        trace(" SQL_DESC_LITERAL_SUFFIX %s", rec->getSuffix());
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, rec->getSuffix());
        break;
    case SQL_DESC_LOCAL_TYPE_NAME:
        trace(" SQL_DESC_LOCAL_TYPE_NAME TODO");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "DataType");
        break;    
    case SQL_DESC_NAME:
        trace(" SQL_DESC_LOCAL_TYPE_NAME");
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, rec->getColumnName().data());
        break;    
    case SQL_DESC_NULLABLE:
        trace(" SQL_DESC_NULLABLE TODO");
        returnInt(NumericAttributePtr, SQL_NULLABLE_UNKNOWN);
        break;
    case SQL_DESC_NUM_PREC_RADIX:
        trace(" SQL_DESC_NUM_PREC_RADIX TODO");
        returnInt(NumericAttributePtr, 10);
        break;
    case SQL_DESC_OCTET_LENGTH:
        trace(" SQL_DESC_OCTET_LENGTH TODO");
        returnInt(NumericAttributePtr, 255);
        break;
    case SQL_DESC_PRECISION:
        trace(" SQL_DESC_PRECISION");
        returnInt(NumericAttributePtr, rec->getPrecision());
        break;
    case SQL_DESC_SCALE:
        trace(" SQL_DESC_SCALE");
        returnInt(NumericAttributePtr, rec->getScale());
        break;  
    case SQL_DESC_SCHEMA_NAME:
        trace(" SQL_DESC_SCHEMA_NAME");        
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "");
        break;
    case SQL_DESC_SEARCHABLE:
        trace(" SQL_DESC_SEARCHABLE");
        returnInt(NumericAttributePtr, SQL_PRED_SEARCHABLE);
        break; 
    case SQL_DESC_TABLE_NAME:
        trace(" SQL_DESC_TABLE_NAME");        
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, rec->getTableName().data());
        break;
    case SQL_DESC_TYPE:
        trace(" SQL_DESC_TYPE %d", rec->getSqlDataType());        
        returnInt(NumericAttributePtr, rec->getSqlDataType());
        break;
    case SQL_DESC_TYPE_NAME:
        trace(" SQL_DESC_TYPE_NAME TODO");        
        returnString((SQLCHAR*)CharacterAttributePtr, BufferLength, StringLengthPtr, "VARCHAR");
        break;
    case SQL_DESC_UNNAMED:
        trace(" SQL_DESC_UNNAMED TODO");        
        returnInt(NumericAttributePtr, SQL_NAMED);
        break;  
    case SQL_DESC_UNSIGNED:
        trace(" SQL_DESC_UNSIGNED");        
        returnInt(NumericAttributePtr, SQL_FALSE);
        break;  
    case SQL_DESC_UPDATABLE:
        trace(" SQL_DESC_UPDATABLE TODO");        
        returnInt(NumericAttributePtr, SQL_ATTR_READONLY);
        break;  
    default:
        trace(" FieldIdentifier=? %d TODO", FieldIdentifier);
        stat->setError(E_HY091);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

