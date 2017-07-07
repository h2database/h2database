import java.sql.*;

public class Sample {

  public static void main(String[] a) throws Exception {
    Class.forName("org.h2.Driver");
    //server connection:
    //Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/rit", "sa", "");
    //embedded connection:
    Connection conn = DriverManager.getConnection("jdbc:h2:~/rit", "sa", "");

    Statement stmt = conn.createStatement();
    stmt.executeUpdate("DROP table if exists rit;");
    stmt.executeUpdate(
        "CREATE table rit ( "
      + "  uno varchar, "
      + "  dos varchar, "
      + "  tres varchar "
      + ");"
    );
    stmt.executeUpdate("create index on rit(uno);");
    stmt.executeUpdate("insert into rit values ('life', 'is', 'good')\n");
    ResultSet rs = stmt.executeQuery("select uno, dos, tres from rit");
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
