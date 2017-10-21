package org.h2.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.h2.expression.Expression;
import org.h2.message.DbException;

public class ColumnNamer {
 
    private static final String DEFAULT_COLUMN_NAME = "DEFAULT";
    private static final String DEFAULT_COMMAND = "DEFAULT";
    private static final String REGULAR_EXPRESSION_MATCH_DISALLOWED = "REGULAR_EXPRESSION_MATCH_DISALLOWED = ";
    private static final String REGULAR_EXPRESSION_MATCH_ALLOWED = "REGULAR_EXPRESSION_MATCH_ALLOWED = ";
    private static final String DEFAULT_COLUMN_NAME_PATTERN = "DEFAULT_COLUMN_NAME_PATTERN = ";
    private static final String MAX_IDENTIFIER_LENGTH = "MAX_IDENTIFIER_LENGTH = ";
    private static final String EMULATE_COMMAND = "EMULATE = ";

    /**
     * Create a standardized column name that isn't null and doesn't have a CR/LF in it.
     * @param columnExp the column expression
     * @param indexOfColumn index of column in below array
     * @return
     */
    public static String getColumnName(Expression expr, int indexOfColumn) {
        return getColumnName(expr,indexOfColumn,(String) null);
    }
    /**
     * Create a standardized column name that isn't null and doesn't have a CR/LF in it.
     * @param columnExp the column expression
     * @param indexOfColumn index of column in below array
     * @param columnNameOverides array of overriding column names
     * @return the new column name
     */
    public static String getColumnName(Expression columnExp, int indexOfColumn, String[] columnNameOverides){
        String columnNameOverride = null;
        if (columnNameOverides != null && columnNameOverides.length > indexOfColumn){
            columnNameOverride = columnNameOverides[indexOfColumn];
        }  
        return getColumnName(columnExp, indexOfColumn, columnNameOverride);
    } 
    
    /**
     * Create a standardized column name that isn't null and doesn't have a CR/LF in it.
     * @param columnExp the column expression
     * @param indexOfColumn index of column in below array
     * @param columnNameOverride single overriding column name
     * @return the new column name
     */
    public static String getColumnName(Expression columnExp, int indexOfColumn, String columnNameOverride) {
        // try a name from the column name override
        String columnName = null;
        if (columnNameOverride != null){
            columnName = columnNameOverride;
            if(!isAllowableColumnName(columnName)){
                columnName = fixColumnName(columnName);
            }            
            if(!isAllowableColumnName(columnName)){
                columnName = null;
            }
        }  
        // try a name from the column alias
        if (columnName==null && columnExp.getAlias()!=null && !DEFAULT_COLUMN_NAME.equals(columnExp.getAlias())){
            columnName = columnExp.getAlias();
            if(!isAllowableColumnName(columnName)){
                columnName = fixColumnName(columnName);
            }
            if(!isAllowableColumnName(columnName)){
                columnName = null;
            }
        }
        // try a name derived from the column expression SQL
        if (columnName==null && columnExp.getColumnName()!=null && !DEFAULT_COLUMN_NAME.equals(columnExp.getColumnName())){
             columnName =  columnExp.getColumnName();
             if(!isAllowableColumnName(columnName)){
                 columnName = fixColumnName(columnName);
             }
             if(!isAllowableColumnName(columnName)){
                 columnName = null;
             }
        }
        // try a name derived from the column expression plan SQL
        if (columnName==null && columnExp.getSQL()!=null && !DEFAULT_COLUMN_NAME.equals(columnExp.getSQL())){
             columnName =  columnExp.getSQL();
             if(!isAllowableColumnName(columnName)){
                 columnName = fixColumnName(columnName);
             }
             if(!isAllowableColumnName(columnName)){
                 columnName = null;
             }
        }
        // go with a innocuous default name pattern
        if (columnName==null){
            columnName =  defaultColumnNamePattern.replace("$$", ""+(indexOfColumn+1));
       }
        return columnName;
    }
        
    public static boolean isAllowableColumnName(String proposedName){
        
        // check null
        if (proposedName == null){
            return false;
        }
        // check size limits
        if (proposedName.length() > maxIdentiferLength || proposedName.length()==0){
            return false;
        }
        Matcher match = compiledRegularExpressionMatchAllowed.matcher(proposedName);
        if(!match.matches()){
            return false;
        }
        return true;
    }
 
    private static String fixColumnName(String proposedName) {
        Matcher match = compiledRegularExpressionMatchDisallowed.matcher(proposedName);
        proposedName = match.replaceAll("");
        
        // check size limits - then truncate
        if (proposedName.length() > maxIdentiferLength){
            proposedName=proposedName.substring(0, maxIdentiferLength);
        }
        
        return proposedName;
    }
    
    private static String unquoteString(String s){
        if(s.startsWith("'") && s.endsWith("'")){
            s = s.substring(1, s.length()-1);
            return s;
        }
        return s;
    }
    
    
    static int maxIdentiferLength = Integer.MAX_VALUE;
    static String regularExpressionMatchAllowed =    "(?m)(?s).+";
    static String regularExpressionMatchDisallowed = "(?m)(?s)[\\x00]";
    static String defaultColumnNamePattern = "_unnamed_column_$$_";
    
    static Pattern compiledRegularExpressionMatchAllowed = Pattern.compile(regularExpressionMatchAllowed);
    static Pattern compiledRegularExpressionMatchDisallowed = Pattern.compile(regularExpressionMatchDisallowed);
    
    public static void configure(String stringValue) {
        try{
            if(stringValue.equalsIgnoreCase(DEFAULT_COMMAND)){
                maxIdentiferLength = Integer.MAX_VALUE;
                regularExpressionMatchAllowed = "(?m)(?s).+";
                regularExpressionMatchDisallowed = "(?m)(?s)[\\x00]";
                defaultColumnNamePattern = "_UNNAMED_$$";
            } else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"ORACLE128")){
                maxIdentiferLength = 128;
                regularExpressionMatchAllowed = "(?m)(?s)\"?[A-Za-z0-9_]+\"?";
                regularExpressionMatchDisallowed = "(?m)(?s)[^A-Za-z0-9_\"]";
                defaultColumnNamePattern = "_UNNAMED_$$";
            } else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"ORACLE30")){
                    maxIdentiferLength = 30;
                    regularExpressionMatchAllowed = "(?m)(?s)\"?[A-Za-z0-9_]+\"?";
                    regularExpressionMatchDisallowed = "(?m)(?s)[^A-Za-z0-9_\"]";
                    defaultColumnNamePattern = "_UNNAMED_$$";
            }else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"POSTGRES")){
                maxIdentiferLength = 31;
                regularExpressionMatchAllowed = "(?m)(?s)\"?[A-Za-z0-9_\"]+\"?";
                regularExpressionMatchDisallowed = "(?m)(?s)[^A-Za-z0-9_\"]";
                defaultColumnNamePattern = "_UNNAMED_$$";
            } else if(stringValue.startsWith(MAX_IDENTIFIER_LENGTH)){
                maxIdentiferLength = Math.max(0,Integer.parseInt(stringValue.substring(MAX_IDENTIFIER_LENGTH.length())));            
            } else if(stringValue.startsWith(DEFAULT_COLUMN_NAME_PATTERN)){
                defaultColumnNamePattern =unquoteString(stringValue.substring(DEFAULT_COLUMN_NAME_PATTERN.length()));            
            } else if(stringValue.startsWith(REGULAR_EXPRESSION_MATCH_ALLOWED)){
                regularExpressionMatchAllowed=unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_ALLOWED.length()));            
            } else if(stringValue.startsWith(REGULAR_EXPRESSION_MATCH_DISALLOWED)){
                regularExpressionMatchDisallowed =unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_DISALLOWED.length()));            
            }
            else
            {
                throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES: unknown id:"+stringValue,
                        stringValue);
    
            }
           
            // recompile RE patterns
            compiledRegularExpressionMatchAllowed = Pattern.compile(regularExpressionMatchAllowed);
            compiledRegularExpressionMatchDisallowed = Pattern.compile(regularExpressionMatchDisallowed);
        }
        //Including NumberFormatException|PatternSyntaxException
        catch(RuntimeException e){
            throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES:"+e.getMessage(),
                    stringValue);

        }
        
    }

}
