/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.spark.sql.service.cli.Type;

/**
 * Column metadata.
 */
public class JdbcColumn {
  private final String columnName;
  private final String tableName;
  private final String tableCatalog;
  private final String type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(String columnName, String tableName, String tableCatalog,
      String type, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableCatalog = tableCatalog;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getType() {
    return type;
  }

  static String columnClassName(Type sparkType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = sparkTypeToSqlType(sparkType);
    switch(columnType) {
      case Types.NULL:
        return "null";
      case Types.BOOLEAN:
        return Boolean.class.getName();
      case Types.CHAR:
      case Types.VARCHAR:
        return String.class.getName();
      case Types.TINYINT:
        return Byte.class.getName();
      case Types.SMALLINT:
        return Short.class.getName();
      case Types.INTEGER:
        return Integer.class.getName();
      case Types.BIGINT:
        return Long.class.getName();
      case Types.DATE:
        return Date.class.getName();
      case Types.FLOAT:
        return Float.class.getName();
      case Types.DOUBLE:
        return Double.class.getName();
      case  Types.TIMESTAMP:
        return Timestamp.class.getName();
      case Types.DECIMAL:
        return BigInteger.class.getName();
      case Types.BINARY:
        return byte[].class.getName();
      case Types.OTHER:
      case Types.JAVA_OBJECT:
        return String.class.getName();
      case Types.ARRAY:
      case Types.STRUCT:
        return String.class.getName();
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static Type typeStringToSparkType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return Type.STRING_TYPE;
    } else if ("float".equalsIgnoreCase(type)) {
      return Type.FLOAT_TYPE;
    } else if ("double".equalsIgnoreCase(type)) {
      return Type.DOUBLE_TYPE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return Type.BOOLEAN_TYPE;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return Type.TINYINT_TYPE;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return Type.SMALLINT_TYPE;
    } else if ("int".equalsIgnoreCase(type)) {
      return Type.INT_TYPE;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return Type.BIGINT_TYPE;
    } else if ("date".equalsIgnoreCase(type)) {
      return Type.DATE_TYPE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return Type.TIMESTAMP_TYPE;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return Type.DECIMAL_TYPE;
    } else if ("binary".equalsIgnoreCase(type)) {
      return Type.BINARY_TYPE;
    } else if ("map".equalsIgnoreCase(type)) {
      return Type.MAP_TYPE;
    } else if ("array".equalsIgnoreCase(type)) {
      return Type.ARRAY_TYPE;
    } else if ("struct".equalsIgnoreCase(type)) {
      return Type.STRUCT_TYPE;
    } else if ("void".equalsIgnoreCase(type) || "null".equalsIgnoreCase(type)) {
      return Type.NULL_TYPE;
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  public static int sparkTypeToSqlType(Type sparkType) throws SQLException {
    return sparkType.toJavaSQLType();
  }

  public static int sparkTypeToSqlType(String type) throws SQLException {
    return sparkTypeToSqlType(typeStringToSparkType(type));
  }

  static String getColumnTypeName(String type) throws SQLException {
    return Type.getType(type).getName();
  }

  static int columnDisplaySize(Type sparkType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    // according to sparkTypeToSqlType possible options are:
    int columnType = sparkTypeToSqlType(sparkType);
    switch(columnType) {
    case Types.NULL:
      return 4; // "NULL"
    case Types.BOOLEAN:
      return columnPrecision(sparkType, columnAttributes);
    case Types.CHAR:
    case Types.VARCHAR:
      return columnPrecision(sparkType, columnAttributes);
    case Types.BINARY:
      return Integer.MAX_VALUE; // spark has no max limit for binary
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      return columnPrecision(sparkType, columnAttributes) + 1; // allow +/-
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
      return columnPrecision(sparkType, columnAttributes);

    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
    case Types.FLOAT:
      return 24; // e.g. -(17#).e-###
    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
    case Types.DOUBLE:
      return 25; // e.g. -(17#).e-####
    case Types.DECIMAL:
      return columnPrecision(sparkType, columnAttributes) + 2;  // '-' sign and '.'
    case Types.OTHER:
    case Types.JAVA_OBJECT:
      return columnPrecision(sparkType, columnAttributes);
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnPrecision(Type sparkType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = sparkTypeToSqlType(sparkType);
    // according to sparkTypeToSqlType possible options are:
    switch(columnType) {
    case Types.NULL:
      return 0;
    case Types.BOOLEAN:
      return 1;
    case Types.CHAR:
    case Types.VARCHAR:
      if (columnAttributes != null) {
        return columnAttributes.precision;
      }
      return Integer.MAX_VALUE; // spark has no max limit for strings
    case Types.BINARY:
      return Integer.MAX_VALUE; // spark has no max limit for binary
    case Types.TINYINT:
      return 3;
    case Types.SMALLINT:
      return 5;
    case Types.INTEGER:
      return 10;
    case Types.BIGINT:
      return 19;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
      return 29;
    case Types.DECIMAL:
      return columnAttributes.precision;
    case Types.OTHER:
    case Types.JAVA_OBJECT:
      return Integer.MAX_VALUE;
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnScale(Type sparkType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = sparkTypeToSqlType(sparkType);
    // according to sparkTypeToSqlType possible options are:
    switch(columnType) {
    case Types.NULL:
    case Types.BOOLEAN:
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.DATE:
    case Types.BINARY:
      return 0;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case  Types.TIMESTAMP:
      return 9;
    case Types.DECIMAL:
      return columnAttributes.scale;
    case Types.OTHER:
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return 0;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public Integer getNumPrecRadix() {
    if (type.equalsIgnoreCase("tinyint")) {
      return 10;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 10;
    } else if (type.equalsIgnoreCase("int")) {
      return 10;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 10;
    } else if (type.equalsIgnoreCase("float")) {
      return 10;
    } else if (type.equalsIgnoreCase("double")) {
      return 10;
    } else if (type.equalsIgnoreCase("decimal")) {
      return 10;
    } else { // anything else including boolean and string is null
      return null;
    }
  }

  public String getComment() {
    return comment;
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }
}
