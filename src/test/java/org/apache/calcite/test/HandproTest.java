/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.adapter.handpro.HandproSchemaFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;


/**
 * Unit test of the Calcite adapter for APIs.
 */
public class HandproTest {
  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
                // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
                // ignore
      }
    }
  }

  @Test public void testNormalJoin() throws SQLException {
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

    while (rs.next())
    {
      System.out.print(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3));
      System.out.print(" " + rs.getInt(4) + " " + rs.getString(5) + " " + rs.getString(6));
    }
    connection.close();
  }
}
