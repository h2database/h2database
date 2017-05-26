package org.h2.jdbc;

import org.h2.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

public class JdbcOptionProperties {

  private static class LazyHolder {
    public static JdbcOptionProperties INSTANCE = new JdbcOptionProperties();
  }

  public final static Data Empty = new Data();

  private final static String OPTIONAL_JVM_PROPERTY = "h2.modeDef";

  private final static String[] MODE_KEYS = {
    "DB2",
    "Derby",
    "HSQLDB",
    "MSSQLServer",
    "MySQL",
    "Oracle",
    "PostgreSQL"
  };

  private final static String MATCH = "MATCH";

  private final static String REPLACE = "REPLACE";

  private final static String DatabaseProductName = "DATABASEPRODUCTNAME";

  private final static String DatabaseMajorVersion = "DATABASEMAJORVERSION";

  private final static String DatabaseProductVersion = "DATABASEPRODUCTVERSION";

  private final static String SQL = "SQL";

  private final static String EMPTY_STRING = "";

  public static JdbcOptionProperties getInstance() {
    return LazyHolder.INSTANCE;
  }

  private final Map<String, Data> outputMap;

  public Data getModeData(String modeKey) {
    return outputMap.getOrDefault(StringUtils.toUpperEnglish(modeKey), Empty);
  }

  private JdbcOptionProperties() {
    this.outputMap = new HashMap<>();
    String pathToPropertyFile = System.getProperty(OPTIONAL_JVM_PROPERTY);
    if(pathToPropertyFile != null) {
      try (InputStream input = new FileInputStream(pathToPropertyFile)) {
        readPropertiesFromFile(input, outputMap);
      }
      catch (IOException ex) {
        //nop
      }
    }
  }


  private static void readPropertiesFromFile(InputStream input, Map<String, Data> outputMap) throws IOException {
    Properties prop = new Properties();
    prop.load(input);

    Map<String, Map<ComposeKey, String>> mapByType = new HashMap<>();
    for(Map.Entry<Object, Object> entry : prop.entrySet()) {
      String[] key = entry.getKey().toString().split("\\.");
      String value = entry.getValue().toString();
      String type = StringUtils.toUpperEnglish(key[0]);
      String[] minorKeyArr = new String[key.length - 1];
      System.arraycopy(key, 1, minorKeyArr, 0, key.length - 1);
      Map<ComposeKey, String> mapByMinorKey = mapByType.computeIfAbsent(type, typeKey -> new HashMap<>());
      mapByMinorKey.put(new ComposeKey(minorKeyArr), value);
    }

    for(String key : MODE_KEYS) {
      String mainKey = StringUtils.toUpperEnglish(key);
      Map<ComposeKey, String> mapByMinorKey = mapByType.get(mainKey);
      if(mapByMinorKey == null) outputMap.put(mainKey, Empty);
      else {
        String databaseProductName=EMPTY_STRING;
        String databaseMajorVersion=EMPTY_STRING;
        String databaseProductVersion=EMPTY_STRING;
        Map<Integer, Map<String, String>> sqlMap = new HashMap<>();
        for(Map.Entry<ComposeKey, String> entry : mapByMinorKey.entrySet()) {
          String[] arr = entry.getKey().getArr();
          String value = entry.getValue();
          switch(StringUtils.toUpperEnglish(arr[0])) {
            case DatabaseProductName :
              databaseProductName = value;
              break;
            case DatabaseMajorVersion :
              databaseMajorVersion = value;
              break;
            case DatabaseProductVersion  :
              databaseProductVersion = value;
              break;
            case SQL :
              if(arr.length == 3) {
                try {
                  int index = Integer.valueOf(arr[1]);
                  String matchOrReplace = StringUtils.toUpperEnglish(arr[2]);
                  Map<String, String> map = sqlMap.computeIfAbsent(index, k -> new HashMap<>());
                  map.put(matchOrReplace, value);
                }
                catch(NumberFormatException e) {
                  //nop
                }
              }
              break;
            default:
              //nop
          }
        }
        int matcherReplacerSize = sqlMap.size();
        Pattern[] matcher = new Pattern[matcherReplacerSize];
        String[] replacer = new String[matcherReplacerSize];
        List<Map<String, String>> sqlValues = new ArrayList<>(sqlMap.values());
        for(int i=0; i < matcherReplacerSize; ++i) {
          matcher[i] = Pattern.compile(sqlValues.get(i).getOrDefault(MATCH, EMPTY_STRING), Pattern.CASE_INSENSITIVE);
          replacer[i] = sqlValues.get(i).getOrDefault(REPLACE, EMPTY_STRING);
        }
        Data data = new Data(
          databaseProductName,
          databaseMajorVersion,
          databaseProductVersion,
          matcher,
          replacer
        );
        outputMap.put(mainKey, data);

      }
    }
  }

  public final static class Data {
    private final String databaseProductName;
    private final String databaseMajorVersion;
    private final String databaseProductVersion;
    private final Pattern[] matcher;
    private final String[] replacer;

    private Data() {
      this.databaseProductName = null;
      this.databaseMajorVersion = null;
      this.databaseProductVersion = null;
      this.matcher = null;
      this.replacer = null;
    }

    private Data(
      String databaseProductName,
      String databaseMajorVersion,
      String databaseProductVersion,
      Pattern[] matcher,
      String[] replacer
    ) {
      this.databaseProductName = databaseProductName;
      this.databaseMajorVersion = databaseMajorVersion;
      this.databaseProductVersion = databaseProductVersion;
      this.matcher = matcher;
      this.replacer = replacer;
    }

    public String getDatabaseProductName() {
      return databaseProductName;
    }

    public String getDatabaseMajorVersion() {
      return databaseMajorVersion;
    }

    public String getDatabaseProductVersion() {
      return databaseProductVersion;
    }

    public Pattern[] getMatcher() {
      return matcher;
    }

    public String[] getReplacer() {
      return replacer;
    }
  }



  private final static class ComposeKey {
    private final String[] arr;

    public ComposeKey(String[] arr) {
      this.arr = arr;
    }

    public String[] getArr() {
      return arr;
    }

    @Override
    public int hashCode() {
      int h = 0;
      for(int i=0; i < arr.length; ++i) {
        h=31*h+arr[i].hashCode();
      }
      return h;
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof ComposeKey)) return false;
      ComposeKey other = (ComposeKey) obj;
      if(arr.length != other.arr.length) return false;
      boolean res = true;
      for(int i = 0; i < arr.length; ++i) {
        res = res & (arr[i]==other.arr[i]);
      }
      return res;
    }
  }
}
