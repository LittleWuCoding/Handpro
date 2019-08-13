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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/** HandproTable.
 *
 */
public abstract class HandproTable extends AbstractTable {
  protected final RelProtoDataType protoRowType;
  protected final String tablename;
  protected  List<HandproFieldType> fieldTypes;

  public HandproTable(String tablename, RelProtoDataType protoRowType) {
    super();
    this.tablename = tablename;
    this.protoRowType = protoRowType;
  }

  /* getRowType from interface Table */
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return getTableRowAndType((JavaTypeFactory) typeFactory, fieldTypes);
    } else {
      return getTableRowAndType((JavaTypeFactory) typeFactory, null);
    }
  }

  private RelDataType getTableRowAndType(JavaTypeFactory typeFactory,
                                         List<HandproFieldType> fieldTypes) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    String[][] strings = HandproAPI.gettableROwandType(tablename);
    for (int i = 0; i < strings.length; i++) {
      names.add(strings[i][0]);

      HandproFieldType fieldType = HandproFieldType.of(strings[i][1]);
      RelDataType type;
      if (fieldType == null) {
        type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      } else {
        type = fieldType.toType(typeFactory);
      }
      types.add(type);
      if (fieldTypes != null) {
        fieldTypes.add(fieldType);
      }
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }
}
// End HandproTable.java
