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


import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Enumerator that reads from a API.
 * @param <E> Row Type
 */
public class HandproEnumerator<E> implements Enumerator<E> {
  private E current;
  private final String tableName;
  private final AtomicBoolean cancelFlag;
  private final String[] filterValues;
  private final RowConverter<E> rowConverter;
  private int rowIndex = 0;
  private int sumOfRow;
  private String[][] tableReader;

  public HandproEnumerator(String tableName, AtomicBoolean cancelFlag,
           String[] filterValues, RowConverter<E> rowConverter) {
    super();
    this.tableName = tableName;
    this.cancelFlag = cancelFlag;
    this.filterValues = filterValues;
    this.rowConverter = rowConverter;
    /* Open the table */
    HandproAPI.tableOpen(tableName);
    this.tableReader = HandproAPI.gettableData(tableName);
    this.sumOfRow = this.tableReader.length;
  }

  public E current() {
    /* return current tuple */
    return current;
  }

  public boolean moveNext() {
    /* return if is there tuples left to be read,
     * if true, set the variable current
     */
    if (this.sumOfRow == 0 || this.tableReader == null
            || this.rowIndex >= this.sumOfRow) {
      current = null;
      return false;
    }

    final String[] strings = this.tableReader[this.rowIndex];
    this.rowIndex = this.rowIndex + 1;
    if (strings == null) {
      current = null;
      return false;
    }
    current = rowConverter.convertRow(strings);
    return true;

  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  public void close() {
    /* Closes this enumerable and releases resources. */
    if (this.tableReader != null) {
      this.tableReader = null;
    }
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);

    protected Object convert(HandproFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case BYTE:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SHORT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INT:
        if (string.length() == 0) {
          return null;
        }
        return Integer.parseInt(string);
      case LONG:
        if (string.length() == 0) {
          return null;
        }
        return Long.parseLong(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      case STRING:
      default:
        return string;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final HandproFieldType[] fieldTypes;
    private final int[] fields;

    ArrayRowConverter(List<HandproFieldType> fieldTypes, int[] fields) {
      this.fields = fields;
      this.fieldTypes = fieldTypes.toArray(new HandproFieldType[0]);
    }

    public Object[] convertRow(String[] strings) {
      final Object[] objects = new Object[strings.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }
  /**xxx
   *
   */
  static class SingleRowConverter extends RowConverter {
    private final HandproFieldType fieldType;
    private final int fieldIndex;

    SingleRowConverter(HandproFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
// End HandproEnumerator.java
