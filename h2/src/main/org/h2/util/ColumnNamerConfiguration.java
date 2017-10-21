package org.h2.util;

import java.util.regex.Pattern;
import org.h2.message.DbException;

public class ColumnNamerConfiguration {
    private static final String DEFAULT_COMMAND = "DEFAULT";
    private static final String REGULAR_EXPRESSION_MATCH_DISALLOWED = "REGULAR_EXPRESSION_MATCH_DISALLOWED = ";
    private static final String REGULAR_EXPRESSION_MATCH_ALLOWED = "REGULAR_EXPRESSION_MATCH_ALLOWED = ";
    private static final String DEFAULT_COLUMN_NAME_PATTERN = "DEFAULT_COLUMN_NAME_PATTERN = ";
    private static final String MAX_IDENTIFIER_LENGTH = "MAX_IDENTIFIER_LENGTH = ";
    private static final String EMULATE_COMMAND = "EMULATE = ";
    private static final String GENERATE_UNIQUE_COLUMN_NAMES = "GENERATE_UNIQUE_COLUMN_NAMES = ";
    
    private int maxIdentiferLength;
    private String regularExpressionMatchAllowed;
    private String regularExpressionMatchDisallowed;
    private String defaultColumnNamePattern;
    private boolean generateUniqueColumnNames;
    private Pattern compiledRegularExpressionMatchAllowed;
    private Pattern compiledRegularExpressionMatchDisallowed;

    public ColumnNamerConfiguration(int maxIdentiferLength, String regularExpressionMatchAllowed,
            String regularExpressionMatchDisallowed, String defaultColumnNamePattern, boolean generateUniqueColumnNames) {
        
        this.maxIdentiferLength = maxIdentiferLength;
        this.regularExpressionMatchAllowed = regularExpressionMatchAllowed;
        this.regularExpressionMatchDisallowed = regularExpressionMatchDisallowed;
        this.defaultColumnNamePattern = defaultColumnNamePattern;
        this.generateUniqueColumnNames = generateUniqueColumnNames;
        
        compiledRegularExpressionMatchAllowed = Pattern.compile(regularExpressionMatchAllowed);
        compiledRegularExpressionMatchDisallowed = Pattern.compile(regularExpressionMatchDisallowed);
    }

    public int getMaxIdentiferLength() {
        return maxIdentiferLength;
    }

    public void setMaxIdentiferLength(int maxIdentiferLength) {
        this.maxIdentiferLength = maxIdentiferLength;
    }

    public String getRegularExpressionMatchAllowed() {
        return regularExpressionMatchAllowed;
    }

    public void setRegularExpressionMatchAllowed(String regularExpressionMatchAllowed) {
        this.regularExpressionMatchAllowed = regularExpressionMatchAllowed;
    }

    public String getRegularExpressionMatchDisallowed() {
        return regularExpressionMatchDisallowed;
    }

    public void setRegularExpressionMatchDisallowed(String regularExpressionMatchDisallowed) {
        this.regularExpressionMatchDisallowed = regularExpressionMatchDisallowed;
    }

    public String getDefaultColumnNamePattern() {
        return defaultColumnNamePattern;
    }

    public void setDefaultColumnNamePattern(String defaultColumnNamePattern) {
        this.defaultColumnNamePattern = defaultColumnNamePattern;
    }

    public Pattern getCompiledRegularExpressionMatchAllowed() {
        return compiledRegularExpressionMatchAllowed;
    }

    public void setCompiledRegularExpressionMatchAllowed(Pattern compiledRegularExpressionMatchAllowed) {
        this.compiledRegularExpressionMatchAllowed = compiledRegularExpressionMatchAllowed;
    }

    public Pattern getCompiledRegularExpressionMatchDisallowed() {
        return compiledRegularExpressionMatchDisallowed;
    }

    public void setCompiledRegularExpressionMatchDisallowed(Pattern compiledRegularExpressionMatchDisallowed) {
        this.compiledRegularExpressionMatchDisallowed = compiledRegularExpressionMatchDisallowed;
    }
    
    public void configure(String stringValue) {
        try{
            if(stringValue.equalsIgnoreCase(DEFAULT_COMMAND)){
                setMaxIdentiferLength(Integer.MAX_VALUE);
                setRegularExpressionMatchAllowed("(?m)(?s).+");
                setRegularExpressionMatchDisallowed("(?m)(?s)[\\x00]");
                setDefaultColumnNamePattern("_UNNAMED_$$");
            } else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"ORACLE128")){
                setMaxIdentiferLength(128);
                setRegularExpressionMatchAllowed("(?m)(?s)\"?[A-Za-z0-9_]+\"?");
                setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\"]");
                setDefaultColumnNamePattern("_UNNAMED_$$");
            } else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"ORACLE30")){
                    setMaxIdentiferLength(30);
                    setRegularExpressionMatchAllowed("(?m)(?s)\"?[A-Za-z0-9_]+\"?");
                    setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\"]");
                    setDefaultColumnNamePattern("_UNNAMED_$$");
            }else if(stringValue.equalsIgnoreCase(EMULATE_COMMAND+"POSTGRES")){
                setMaxIdentiferLength(31);
                setRegularExpressionMatchAllowed("(?m)(?s)\"?[A-Za-z0-9_\"]+\"?");
                setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\"]");
                setDefaultColumnNamePattern("_UNNAMED_$$");
            } else if(stringValue.startsWith(MAX_IDENTIFIER_LENGTH)){
                int maxLength = Integer.parseInt(stringValue.substring(MAX_IDENTIFIER_LENGTH.length()));
                setMaxIdentiferLength(Math.max(30,maxLength));
                if(maxLength!=getMaxIdentiferLength()){
                    throw DbException.getInvalidValueException("Illegal value (<30) in SET COLUMN_NAME_RULES="+stringValue,stringValue);
                }
            } else if(stringValue.startsWith(GENERATE_UNIQUE_COLUMN_NAMES)){
                setGenerateUniqueColumnNames(Integer.parseInt(stringValue.substring(GENERATE_UNIQUE_COLUMN_NAMES.length()))==1);            
            } else if(stringValue.startsWith(DEFAULT_COLUMN_NAME_PATTERN)){
                setDefaultColumnNamePattern(unquoteString(stringValue.substring(DEFAULT_COLUMN_NAME_PATTERN.length())));            
            } else if(stringValue.startsWith(REGULAR_EXPRESSION_MATCH_ALLOWED)){
                setRegularExpressionMatchAllowed(unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_ALLOWED.length())));            
            } else if(stringValue.startsWith(REGULAR_EXPRESSION_MATCH_DISALLOWED)){
                setRegularExpressionMatchDisallowed(unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_DISALLOWED.length())));            
            }
            else
            {
                throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES: unknown id:"+stringValue,
                        stringValue);
    
            }
           
            // recompile RE patterns
            setCompiledRegularExpressionMatchAllowed(Pattern.compile(getRegularExpressionMatchAllowed()));
            setCompiledRegularExpressionMatchDisallowed(Pattern.compile(getRegularExpressionMatchDisallowed()));
        }
        //Including NumberFormatException|PatternSyntaxException
        catch(RuntimeException e){
            throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES:"+e.getMessage(),
                    stringValue);

        }        
    }    
    
    public static ColumnNamerConfiguration getDefault(){
        return new ColumnNamerConfiguration(Integer.MAX_VALUE, "(?m)(?s).+", "(?m)(?s)[\\x00]", "_unnamed_column_$$_",false);
    }
    
    private static String unquoteString(String s){
        if(s.startsWith("'") && s.endsWith("'")){
            s = s.substring(1, s.length()-1);
            return s;
        }
        return s;
    }

    public boolean isGenerateUniqueColumnNames() {
        return generateUniqueColumnNames;
    }

    public void setGenerateUniqueColumnNames(boolean generateUniqueColumnNames) {
        this.generateUniqueColumnNames = generateUniqueColumnNames;
    }
        
}