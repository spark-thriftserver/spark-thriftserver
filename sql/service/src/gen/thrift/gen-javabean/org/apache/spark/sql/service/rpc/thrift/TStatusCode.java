/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.spark.sql.service.rpc.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum TStatusCode implements org.apache.thrift.TEnum {
  SUCCESS_STATUS(0),
  SUCCESS_WITH_INFO_STATUS(1),
  STILL_EXECUTING_STATUS(2),
  ERROR_STATUS(3),
  INVALID_HANDLE_STATUS(4);

  private final int value;

  private TStatusCode(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TStatusCode findByValue(int value) { 
    switch (value) {
      case 0:
        return SUCCESS_STATUS;
      case 1:
        return SUCCESS_WITH_INFO_STATUS;
      case 2:
        return STILL_EXECUTING_STATUS;
      case 3:
        return ERROR_STATUS;
      case 4:
        return INVALID_HANDLE_STATUS;
      default:
        return null;
    }
  }
}
