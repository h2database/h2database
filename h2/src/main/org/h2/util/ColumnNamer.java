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
            if(!isReasonableColumnName(columnName)){
                columnName = null;
            }
        }
        if (columnName==null && columnExp.getColumnName()!=null){
             columnName =  columnExp.getColumnName();
             if(!isReasonableColumnName(columnName)){
                 columnName = null;
             }
        }
        if (columnName==null){
            columnName =  "_unnamed_column_"+(indexOfColumn+1)+"_";
       }
        return columnName;
    }
    
    private static Pattern reasonableNamePatternRE = Pattern.compile("[A-Z_][A-Z0-9_]*");

    public static boolean isReasonableColumnName(String proposedName){
        if (proposedName == null){
            return false;
        }
        Matcher m = reasonableNamePatternRE.matcher(proposedName.toUpperCase());
        boolean isReasonableName = m.matches();
        return isReasonableName;
    }

}
