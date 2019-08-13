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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;


import java.util.Map;

/** Schema that reads from a API.
 *
 */
public class HandproSchema extends AbstractSchema {
  private Map<String, Table> tableMap;
  private HandproTable.Flavor flavor;

  public HandproSchema(HandproTable.Flavor flavor) {
    super();
    this.flavor = flavor;
  }

  /**
   * @return Map of tables in this schema by name
   */
  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    String tablename = "TEST1";
    Table tbl = createTable(tablename);
    builder.put(tablename, tbl);

    tablename = "TEST2";
    tbl = createTable(tablename);
    builder.put(tablename, tbl);

    return builder.build();
  }

  private Table createTable(String tablename) {
    switch (this.flavor) {
    case SCANNABLE:
      return new HandproScannableTable(tablename, null);
    case FILTERABLE:
    case TRANSLATABLE:
    default:
      return new HandproScannableTable(tablename, null);
    }
  }
}
// End HandproSchema.java
