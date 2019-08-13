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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;

import java.util.concurrent.atomic.AtomicBoolean;

/** xxx
 *
 */
public class HandproScannableTable extends HandproTable implements ScannableTable  {

  HandproScannableTable(String tablename, RelProtoDataType protoRowType) {
    super(tablename, protoRowType);
  }

  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = HandproEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new HandproEnumerator<>(tablename, cancelFlag, null,
          new HandproEnumerator.ArrayRowConverter(fieldTypes, fields));
        }
      };
  }
}
// End HandproScannableTable.java
