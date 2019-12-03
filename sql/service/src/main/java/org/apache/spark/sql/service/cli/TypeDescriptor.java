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

package org.apache.spark.sql.service.cli;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.sql.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.spark.sql.service.rpc.thrift.TTypeDesc;
import org.apache.spark.sql.service.rpc.thrift.TTypeEntry;
import org.apache.spark.sql.types.DecimalType;

/**
 * TypeDescriptor.
 *
 */
public class TypeDescriptor {

  private final Type type;
  private String typeName = null;
  private TypeQualifiers typeQualifiers = null;
  private static ConcurrentHashMap<String, Type> cachedPrimitiveTypeInfo = new ConcurrentHashMap();

  static {
    cachedPrimitiveTypeInfo.put("null", Type.NULL_TYPE);
    cachedPrimitiveTypeInfo.put("boolean", Type.BOOLEAN_TYPE);
    cachedPrimitiveTypeInfo.put("int", Type.INT_TYPE);
    cachedPrimitiveTypeInfo.put("bigint", Type.BIGINT_TYPE);
    cachedPrimitiveTypeInfo.put("string", Type.STRING_TYPE);
    cachedPrimitiveTypeInfo.put("float", Type.FLOAT_TYPE);
    cachedPrimitiveTypeInfo.put("double", Type.DOUBLE_TYPE);
    cachedPrimitiveTypeInfo.put("tinyint", Type.TINYINT_TYPE);
    cachedPrimitiveTypeInfo.put("smallint", Type.SMALLINT_TYPE);
    cachedPrimitiveTypeInfo.put("date", Type.DATE_TYPE);
    cachedPrimitiveTypeInfo.put("timestamp", Type.TIMESTAMP_TYPE);
    cachedPrimitiveTypeInfo.put("binary", Type.BINARY_TYPE);
    cachedPrimitiveTypeInfo.put("decimal", Type.DECIMAL_TYPE);
  }

  public TypeDescriptor(Type type) {
    this.type = type;
  }

  public TypeDescriptor(TTypeDesc tTypeDesc) {
    List<TTypeEntry> tTypeEntries = tTypeDesc.getTypes();
    TPrimitiveTypeEntry top = tTypeEntries.get(0).getPrimitiveEntry();
    this.type = Type.getType(top.getType());
    if (top.isSetTypeQualifiers()) {
      setTypeQualifiers(TypeQualifiers.fromTTypeQualifiers(top.getTypeQualifiers()));
    }
  }

  public TypeDescriptor(String typeName) {
    this.type = Type.getType(typeName);
    if (this.type.isComplexType()) {
      this.typeName = typeName;
    } else if (this.type.isQualifiedType()) {
      Type type = getTypeInfo(typeName);
      setTypeQualifiers(TypeQualifiers.fromTypeInfo(type));
    }
  }

  public Type getTypeInfo(String typeName) {
    Type type = cachedPrimitiveTypeInfo.getOrDefault(typeName, Type.STRING_TYPE);
    if (typeName.equalsIgnoreCase("decimal")) {
      // Todo add parser for decimal(m,n), but seem spark won't get this.
      type.setSpecifiedPrecision(DecimalType.MAX_PRECISION());
      type.setSpecifiedScala(DecimalType.MAX_SCALE());
    }
    return type;
  }

  public Type getType() {
    return type;
  }

  public TTypeDesc toTTypeDesc() {
    TPrimitiveTypeEntry primitiveEntry = new TPrimitiveTypeEntry(type.toTType());
    if (getTypeQualifiers() != null) {
      primitiveEntry.setTypeQualifiers(getTypeQualifiers().toTTypeQualifiers());
    }
    TTypeEntry entry = TTypeEntry.primitiveEntry(primitiveEntry);

    TTypeDesc desc = new TTypeDesc();
    desc.addToTypes(entry);
    return desc;
  }

  public String getTypeName() {
    if (typeName != null) {
      return typeName;
    } else {
      return type.getName();
    }
  }

  public TypeQualifiers getTypeQualifiers() {
    return typeQualifiers;
  }

  public void setTypeQualifiers(TypeQualifiers typeQualifiers) {
    this.typeQualifiers = typeQualifiers;
  }

  /**
   * The column size for this type.
   * For numeric data this is the maximum precision.
   * For character data this is the length in characters.
   * For datetime types this is the length in characters of the String representation
   * (assuming the maximum allowed precision of the fractional seconds component).
   * For binary data this is the length in bytes.
   * Null is returned for data types where the column size is not applicable.
   */
  public Integer getColumnSize() {
    if (type.isNumericType()) {
      return getPrecision();
    }
    switch (type) {
    case STRING_TYPE:
    case BINARY_TYPE:
      return Integer.MAX_VALUE;
    case DATE_TYPE:
      return 10;
    case TIMESTAMP_TYPE:
      return 29;
    default:
      return null;
    }
  }

  /**
   * Maximum precision for numeric types.
   * Returns null for non-numeric types.
   * @return
   */
  public Integer getPrecision() {
    if (this.type == Type.DECIMAL_TYPE) {
      return typeQualifiers.getPrecision();
    }
    return this.type.getMaxPrecision();
  }

  /**
   * The number of fractional digits for this type.
   * Null is returned for data types where this is not applicable.
   */
  public Integer getDecimalDigits() {
    switch (this.type) {
    case BOOLEAN_TYPE:
    case TINYINT_TYPE:
    case SMALLINT_TYPE:
    case INT_TYPE:
    case BIGINT_TYPE:
      return 0;
    case FLOAT_TYPE:
      return 7;
    case DOUBLE_TYPE:
      return 15;
    case DECIMAL_TYPE:
      return typeQualifiers.getScale();
    case TIMESTAMP_TYPE:
      return 9;
    default:
      return null;
    }
  }


}
