package org.h2.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.h2.expression.Expression;

public class ColumnNamer {
    
    public static String getColumnName(Expression columnExp, int indexOfColumn, String[] columnNameOverides){
        String columnName = null;
        if (columnNameOverides != null && columnNameOverides.length > indexOfColumn){
            columnName = columnNameOverides[indexOfColumn];
        }  
        if (columnName==null && columnExp.getAlias()!=null){
            columnName = columnExp.getAlias();
            if(!isAllowableColumnName(columnName)){
                columnName = null;
            }
        }
        if (columnName==null && columnExp.getColumnName()!=null){
             columnName =  columnExp.getColumnName();
             if(!isAllowableColumnName(columnName)){
                 columnName = columnName.replace('\n', ' ').replace('\r', ' ');
             }
             if(!isAllowableColumnName(columnName)){
                 columnName = null;
             }
        }
        if (columnName==null){
            columnName =  "_unnamed_column_"+(indexOfColumn+1)+"_";
       }
        return columnName;
    }
    
    //private static final Pattern reasonableNameCharactersPatternRE = Pattern.compile("[a-zA-Z0-9_'\\(\\)\\*,\\.\\+\\-\\*/:=\\<\\>!\\|@ \\t\\?\"\\$]*");
    
    public static boolean isAllowableColumnName(String proposedName){
        if (proposedName == null){
            return false;
        }
        if(proposedName.contains("\n") || proposedName.contains("\r")){
            return false;
        }
        return true;
    }

}
