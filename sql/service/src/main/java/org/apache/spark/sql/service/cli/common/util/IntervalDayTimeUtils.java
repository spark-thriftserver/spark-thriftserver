//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.spark.sql.service.cli.common.util;

import org.apache.spark.sql.service.cli.common.type.SparkIntervalDayTime;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

public class IntervalDayTimeUtils {
  private static final ThreadLocal<SimpleDateFormat> dateFormatLocal = new ThreadLocal<SimpleDateFormat>() {
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd");
    }
  };
  public static final int NANOS_PER_SEC = 1000000000;
  public static final BigDecimal MAX_INT_BD = new BigDecimal(2147483647);
  public static final BigDecimal NANOS_PER_SEC_BD = new BigDecimal(1000000000);

  public IntervalDayTimeUtils() {
  }

  public static SimpleDateFormat getDateFormat() {
    return (SimpleDateFormat)dateFormatLocal.get();
  }

  public static int parseNumericValueWithRange(String fieldName, String strVal, int minValue, int maxValue) throws IllegalArgumentException {
    int result = 0;
    if (strVal != null) {
      result = Integer.parseInt(strVal);
      if (result < minValue || result > maxValue) {
        throw new IllegalArgumentException(String.format("%s value %d outside range [%d, %d]", fieldName, result, minValue, maxValue));
      }
    }

    return result;
  }

  public static long getIntervalDayTimeTotalNanos(SparkIntervalDayTime intervalDayTime) {
    return intervalDayTime.getTotalSeconds() * 1000000000L + (long)intervalDayTime.getNanos();
  }

  public static void setIntervalDayTimeTotalNanos(SparkIntervalDayTime intervalDayTime, long totalNanos) {
    intervalDayTime.set(totalNanos / 1000000000L, (int)(totalNanos % 1000000000L));
  }

  public static long getIntervalDayTimeTotalSecondsFromTotalNanos(long totalNanos) {
    return totalNanos / 1000000000L;
  }

  public static int getIntervalDayTimeNanosFromTotalNanos(long totalNanos) {
    return (int)(totalNanos % 1000000000L);
  }
}
