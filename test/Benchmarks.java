import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.lang.Math;


public class Benchmarks {

    

  public static void main(String[] a) throws Exception {
    Class.forName("org.h2.Driver");
    Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/rit", "sa", "");

    System.out.println("initializing...");

    List<String> words1 = Arrays.asList(
        "Sed", "ut", "perspiciatis", "unde", "omnis", "iste", "natus", "error", "sit"
      , "voluptatem", "accusantium", "doloremque", "laudantium,", "totam", "rem"
      , "aperiam,"
      , "eaque", "ipsa", "quae", "ab", "illo", "inventore", "veritatis", "et", "quasi"
      , "architecto", "beatae", "vitae", "dicta", "sunt", "explicabo.", "Nemo", "enim"
      , "ipsam", "voluptatem", "quia", "voluptas", "sit", "aspernatur", "aut", "odit"
      , "tempora"
      , "incidunt", "ut", "labore", "et", "dolore", "magnam", "aliquam", "quaerat"
      , "voluptatem.", "Ut", "enim", "ad", "minima", "veniam,", "quis", "nostrum"
      , "Sed", "ut", "perspiciatis", "unde", "omnis", "iste", "natus", "error", "sit"
      , "voluptatem", "accusantium", "doloremque", "laudantium,", "totam", "rem"
      , "aperiam,"
      , "eaque", "ipsa", "quae", "ab", "illo", "inventore", "veritatis", "et", "quasi"
    );
    List<String> words2 = Arrays.asList(
        "est,"
      , "qui", "dolorem", "ipsum", "quia", "dolor", "sit", "amet,", "consectetur,"
      , "adipisci", "velit,", "sed", "quia", "non", "numquam", "eius", "modi"
      , "tempora"
      , "incidunt", "ut", "labore", "et", "dolore", "magnam", "aliquam", "quaerat"
      , "voluptatem.", "Ut", "enim", "ad", "minima", "veniam,", "quis", "nostrum"
      , "Sed", "ut", "perspiciatis", "unde", "omnis", "iste", "natus", "error", "sit"
      , "voluptatem", "accusantium", "doloremque", "laudantium,", "totam", "rem"
      , "aperiam,"
      , "eaque", "ipsa", "quae", "ab", "illo", "inventore", "veritatis", "et", "quasi"
      , "architecto", "beatae", "vitae", "dicta", "sunt", "explicabo.", "Nemo", "enim"
      , "ipsam", "voluptatem", "quia", "voluptas", "sit", "aspernatur", "aut", "odit"
    );
    List<String> words3 = Arrays.asList(
        "exercitationem", "ullam", "corporis", "suscipit", "laboriosam,", "nisi", "ut"
      , "aliquid", "ex", "ea", "commodi", "consequatur?", "Quis", "autem", "vel", "eum"
      , "iure", "reprehenderit", "qui", "in", "ea", "voluptate", "velit", "esse"
      , "quam"
      , "fugiat", "quo", "voluptas", "nulla", "pariatur"
      , "exercitationem", "ullam", "corporis", "suscipit", "laboriosam,", "nisi", "ut"
      , "aliquid", "ex", "ea", "commodi", "consequatur?", "Quis", "autem", "vel", "eum"
      , "iure", "reprehenderit", "qui", "in", "ea", "voluptate", "velit", "esse"
      , "quam"
      , "fugiat", "quo", "voluptas", "nulla", "pariatur"
    );

    String insertSql = "insert into rit values \n";
    int l1 = words1.size();
    int l2 = words2.size();
    int l3 = words3.size();
    int l = Math.min(l1, Math.min(l2, l3));
    for (int i = 0; i < l; i++) {
      String w1 = i % 3 == 0 ? words1.get(i).toUpperCase() : words1.get(i).toLowerCase();
      String w2 = i % 4 == 0 ? words2.get(i).toUpperCase() : words2.get(i).toLowerCase();
      String w3 = i % 5 == 0 ? words3.get(i).toUpperCase() : words3.get(i).toLowerCase();

      insertSql += ("('" + w1 + "', '" + w2 + "', '" + w3 + "')");
            
      if (i == l - 1) {
        insertSql += ";\n";
      } else {
        insertSql += ",\n";
      }
    }


    String q = "select uno, dos, tres from rit";


    Statement stmt = conn.createStatement();
    stmt.executeUpdate("DROP table if exists rit;");
    stmt.executeUpdate(
        "CREATE table rit ( \n"
      + "  uno varchar, \n"
      + "  dos varchar, \n"
      + "  tres varchar \n"
      + ");"
    );
    stmt.executeUpdate("Create index on rit(uno);");
    stmt.executeUpdate(insertSql);
    ResultSet rs = stmt.executeQuery(q);
    while (rs.next()) {
      String uno = rs.getString("uno");
      String dos = rs.getString("dos");
      String tres = rs.getString("tres");

      System.out.println("uno: " + uno + ", dos: " + dos + ", tres: " + tres);
    }
    rs.close();

    conn.close();
  }
}
