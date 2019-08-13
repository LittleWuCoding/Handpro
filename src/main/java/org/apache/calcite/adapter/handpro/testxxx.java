package org.apache.calcite.adapter.handpro;

import java.sql.*;
import java.util.Properties;
import org.apache.calcite.adapter.handpro.HandproSchemaFactory;


public class testxxx {
    public static void main( String[] args ) throws NoSuchMethodException, SecurityException, SQLException {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        Properties info = new Properties();
        info.put("model",
                "inline:"
                        + "{"
                        +"  'version': '1.0',"
                        +"  'defaultSchema': 'SALES',"
                        +"  'schemas': ["
                        +"    {"
                        +"      'name': 'SALES',"
                        +"      'type': 'custom',"
                        +"      'factory': 'org.apache.calcite.adapter.handpro.HandproSchemaFactory',"
                        +"      'operand': {"
                        +"           'flavor': 'scannable'"
                        +"      }"
                        +"    }"
                        +"  ]"
                        +"}");

        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        ResultSet tables =
                connection.getMetaData().getTables(null, null, null, null);
        System.out.println(tables.next());
        tables.close();
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select * from TEST1 ,TEST2 where TEST1.\"pto\" = TEST2.\"age\"");

        System.out.println("YES11");

        while (rs.next())
        {
            System.out.print(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3));
            System.out.print(" " + rs.getInt(4) + " " + rs.getString(5) + " " + rs.getString(6));
        }
        connection.close();
    }
}
