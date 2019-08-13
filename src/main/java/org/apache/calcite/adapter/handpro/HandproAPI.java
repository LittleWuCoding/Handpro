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
package org.apache.calcite.adapter.handpro;
/** xxxxx
 */
public class HandproAPI {
  private HandproAPI() {

  }
  public static void  tableOpen(String tablename) {

  }

  public static String[][] gettableData(String tablename) {
    String[][] data = new String[2][3];

    if (tablename.equalsIgnoreCase("test1")) {
      data[0][0] = "1";
      data[0][1] = "lisi";
      data[0][2] = "17";

      data[1][0] = "2";
      data[1][1] = "zhangsan";
      data[1][2] = "28";
    } else if (tablename.equalsIgnoreCase("test2")) {
      data[0][0] = "1";
      data[0][1] = "xxx";
      data[0][2] = "2";

      data[1][0] = "2";
      data[1][1] = "aaa";
      data[1][2] = "17";
    }

    return data;
  }

  public  static String[][] gettableROwandType(String tablename) {
    String[][] data = new String[3][2];
    data[0][0] = "pto";
    data[0][1] = "int";
    data[1][0] = "name";
    data[1][1] = "string";
    data[2][0] = "age";
    data[2][1] = "int";
    return data;
  }
}
// End HandproAPI.java
