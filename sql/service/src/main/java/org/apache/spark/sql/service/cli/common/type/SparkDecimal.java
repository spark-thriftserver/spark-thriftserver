//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.spark.sql.service.cli.common.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public final class SparkDecimal extends FastSparkDecimal implements Comparable<SparkDecimal> {
  
  public static final int MAX_PRECISION = 38;
  
  public static final int MAX_SCALE = 38;
  
  public static final int USER_DEFAULT_PRECISION = 10;
  
  public static final int USER_DEFAULT_SCALE = 0;
  
  public static final int SYSTEM_DEFAULT_PRECISION = 38;
  
  public static final int SYSTEM_DEFAULT_SCALE = 18;
  
  public static final SparkDecimal ZERO = create(0);
  
  public static final SparkDecimal ONE = create(1);
  
  public static final int ROUND_FLOOR = 3;
  
  public static final int ROUND_CEILING = 2;
  
  public static final int ROUND_HALF_UP = 4;
  
  public static final int ROUND_HALF_EVEN = 6;
  
  public static final int SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ = 24;
  
  public static final int SCRATCH_LONGS_LEN = 6;
  
  public static final int SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES = 49;
  
  public static final int SCRATCH_BUFFER_LEN_TO_BYTES = 79;

  private SparkDecimal() {
  }

  private SparkDecimal(SparkDecimal dec) {
    super(dec);
  }

  private SparkDecimal(FastSparkDecimal fastDec) {
    super(fastDec);
  }

  private SparkDecimal(int fastSignum, FastSparkDecimal fastDec) {
    super(fastSignum, fastDec);
  }

  private SparkDecimal(int fastSignum, long fast0, long fast1, long fast2, int fastIntegerDigitCount, int fastScale) {
    super(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  
  public static SparkDecimal createFromFast(FastSparkDecimal fastDec) {
    return new SparkDecimal(fastDec);
  }

  
  public static SparkDecimal create(BigDecimal bigDecimal) {
    return create(bigDecimal, true);
  }

  
  public static SparkDecimal create(BigDecimal bigDecimal, boolean allowRounding) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBigDecimal(bigDecimal, allowRounding) ? null : result;
  }

  
  public static SparkDecimal create(BigInteger bigInteger) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBigInteger(bigInteger) ? null : result;
  }

  
  public static SparkDecimal create(BigInteger bigInteger, int scale) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBigIntegerAndScale(bigInteger, scale) ? null : result;
  }

  
  public static SparkDecimal create(String string) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromString(string, true) ? null : result;
  }

  
  public static SparkDecimal create(String string, boolean trimBlanks) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromString(string, trimBlanks) ? null : result;
  }

  
  public static SparkDecimal create(byte[] bytes) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBytes(bytes, 0, bytes.length, false) ? null : result;
  }

  
  public static SparkDecimal create(byte[] bytes, boolean trimBlanks) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBytes(bytes, 0, bytes.length, trimBlanks) ? null : result;
  }

  
  public static SparkDecimal create(boolean isNegative, byte[] bytes, int scale) {
    SparkDecimal result = new SparkDecimal();
    if (!result.fastSetFromDigitsOnlyBytesAndScale(isNegative, bytes, 0, bytes.length, scale)) {
      return null;
    } else {
      if (isNegative) {
        result.fastNegate();
      }

      return result;
    }
  }

  
  public static SparkDecimal create(boolean isNegative, byte[] bytes, int offset, int length, int scale) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromDigitsOnlyBytesAndScale(isNegative, bytes, offset, length, scale) ? null : result;
  }

  
  public static SparkDecimal create(byte[] bytes, int offset, int length) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBytes(bytes, offset, length, false) ? null : result;
  }

  
  public static SparkDecimal create(byte[] bytes, int offset, int length, boolean trimBlanks) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBytes(bytes, offset, length, trimBlanks) ? null : result;
  }

  
  public static SparkDecimal create(int intValue) {
    SparkDecimal result = new SparkDecimal();
    result.fastSetFromInt(intValue);
    return result;
  }

  
  public static SparkDecimal create(long longValue) {
    SparkDecimal result = new SparkDecimal();
    result.fastSetFromLong(longValue);
    return result;
  }

  
  public static SparkDecimal create(long longValue, int scale) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromLongAndScale(longValue, scale) ? null : result;
  }

  
  public static SparkDecimal create(float floatValue) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromFloat(floatValue) ? null : result;
  }

  
  public static SparkDecimal create(double doubleValue) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromDouble(doubleValue) ? null : result;
  }

  
  public static SparkDecimal serializationUtilsRead(InputStream inputStream, int scale, byte[] scratchBytes) throws IOException {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSerializationUtilsRead(inputStream, scale, scratchBytes) ? null : result;
  }

  
  public static SparkDecimal createFromBigIntegerBytesAndScale(byte[] bytes, int scale) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBigIntegerBytesAndScale(bytes, 0, bytes.length, scale) ? null : result;
  }

  
  public static SparkDecimal createFromBigIntegerBytesAndScale(byte[] bytes, int offset, int length, int scale) {
    SparkDecimal result = new SparkDecimal();
    return !result.fastSetFromBigIntegerBytesAndScale(bytes, offset, length, scale) ? null : result;
  }

  
  public boolean serializationUtilsWrite(OutputStream outputStream, long[] scratchLongs) throws IOException {
    return this.fastSerializationUtilsWrite(outputStream, scratchLongs);
  }

  
  public int bigIntegerBytes(long[] scratchLongs, byte[] buffer) {
    return this.fastBigIntegerBytes(scratchLongs, buffer);
  }

  
  public byte[] bigIntegerBytes() {
    long[] scratchLongs = new long[6];
    byte[] buffer = new byte[49];
    int byteLength = this.fastBigIntegerBytes(scratchLongs, buffer);
    return Arrays.copyOfRange(buffer, 0, byteLength);
  }

  
  public int bigIntegerBytesScaled(int serializeScale, long[] scratchLongs, byte[] buffer) {
    return this.fastBigIntegerBytesScaled(serializeScale, scratchLongs, buffer);
  }

  
  public byte[] bigIntegerBytesScaled(int serializeScale) {
    long[] scratchLongs = new long[6];
    byte[] buffer = new byte[49];
    int byteLength = this.fastBigIntegerBytesScaled(serializeScale, scratchLongs, buffer);
    return Arrays.copyOfRange(buffer, 0, byteLength);
  }

  
  public String toString() {
    return this.fastSerializationScale() != -1 ? this.fastToFormatString(this.fastSerializationScale()) : this.fastToString();
  }

  
  public String toString(byte[] scratchBuffer) {
    return this.fastSerializationScale() != -1 ? this.fastToFormatString(this.fastSerializationScale(), scratchBuffer) : this.fastToString(scratchBuffer);
  }

  
  public String toFormatString(int formatScale) {
    return this.fastToFormatString(formatScale);
  }

  
  public String toFormatString(int formatScale, byte[] scratchBuffer) {
    return this.fastToFormatString(formatScale, scratchBuffer);
  }

  
  public String toDigitsOnlyString() {
    return this.fastToDigitsOnlyString();
  }

  
  public int toBytes(byte[] scratchBuffer) {
    return this.fastToBytes(scratchBuffer);
  }

  
  public int toFormatBytes(int formatScale, byte[] scratchBuffer) {
    return this.fastToFormatBytes(formatScale, scratchBuffer);
  }

  
  public int toDigitsOnlyBytes(byte[] scratchBuffer) {
    return this.fastToDigitsOnlyBytes(scratchBuffer);
  }

  
  public int compareTo(SparkDecimal dec) {
    return this.fastCompareTo(dec);
  }

  
  public int newFasterHashCode() {
    return this.fastNewFasterHashCode();
  }

  
  public int hashCode() {
    return this.fastHashCode();
  }

  
  public boolean equals(Object obj) {
    return obj != null && obj.getClass() == this.getClass() ? this.fastEquals((SparkDecimal)obj) : false;
  }

  
  public int scale() {
    return this.fastScale();
  }

  
  public int integerDigitCount() {
    return this.fastIntegerDigitCount();
  }

  
  public int precision() {
    return this.fastSqlPrecision();
  }

  
  public int rawPrecision() {
    return this.fastRawPrecision();
  }

  
  public int signum() {
    return this.fastSignum();
  }

  
  public boolean isByte() {
    return this.fastIsByte();
  }

  
  public byte byteValue() {
    return this.fastByteValueClip();
  }

  
  public boolean isShort() {
    return this.fastIsShort();
  }

  
  public short shortValue() {
    return this.fastShortValueClip();
  }

  
  public boolean isInt() {
    return this.fastIsInt();
  }

  
  public int intValue() {
    return this.fastIntValueClip();
  }

  
  public boolean isLong() {
    return this.fastIsLong();
  }

  
  public long longValue() {
    return this.fastLongValueClip();
  }

  
  public long longValueExact() {
    if (!this.isLong()) {
      throw new ArithmeticException();
    } else {
      return this.fastLongValueClip();
    }
  }

  
  public float floatValue() {
    return this.fastFloatValue();
  }

  
  public double doubleValue() {
    return this.fastDoubleValue();
  }

  
  public BigDecimal bigDecimalValue() {
    return this.fastBigDecimalValue();
  }

  
  public BigInteger unscaledValue() {
    return this.fastBigIntegerValue();
  }

  
  public SparkDecimal fractionPortion() {
    SparkDecimal result = new SparkDecimal();
    result.fastFractionPortion();
    return result;
  }

  
  public SparkDecimal integerPortion() {
    SparkDecimal result = new SparkDecimal();
    result.fastIntegerPortion();
    return result;
  }

  
  public SparkDecimal add(SparkDecimal dec) {
    SparkDecimal result = new SparkDecimal();
    return !this.fastAdd(dec, result) ? null : result;
  }

  
  public SparkDecimal subtract(SparkDecimal dec) {
    SparkDecimal result = new SparkDecimal();
    return !this.fastSubtract(dec, result) ? null : result;
  }

  
  public SparkDecimal multiply(SparkDecimal dec) {
    SparkDecimal result = new SparkDecimal();
    return !this.fastMultiply(dec, result) ? null : result;
  }

  
  public SparkDecimal scaleByPowerOfTen(int power) {
    if (power != 0 && this.fastSignum() != 0) {
      SparkDecimal result = new SparkDecimal();
      return !this.fastScaleByPowerOfTen(power, result) ? null : result;
    } else {
      return this;
    }
  }

  
  public SparkDecimal abs() {
    if (this.fastSignum() != -1) {
      return this;
    } else {
      SparkDecimal result = new SparkDecimal(this);
      result.fastAbs();
      return result;
    }
  }

  
  public SparkDecimal negate() {
    if (this.fastSignum() == 0) {
      return this;
    } else {
      SparkDecimal result = new SparkDecimal(this);
      result.fastNegate();
      return result;
    }
  }

  /** @deprecated */
  @Deprecated
  
  public SparkDecimal setScale(int serializationScale) {
    SparkDecimal result = new SparkDecimal(this);
    result.fastSetSerializationScale(serializationScale);
    return result;
  }

  
  public SparkDecimal setScale(int roundingPoint, int roundingMode) {
    if (this.fastScale() == roundingPoint) {
      return this;
    } else {
      SparkDecimal result = new SparkDecimal();
      return !this.fastRound(roundingPoint, roundingMode, result) ? null : result;
    }
  }

  
  public SparkDecimal pow(int exponent) {
    SparkDecimal result = new SparkDecimal(this);
    return !this.fastPow(exponent, result) ? null : result;
  }

  
  public SparkDecimal divide(SparkDecimal divisor) {
    SparkDecimal result = new SparkDecimal();
    return !this.fastDivide(divisor, result) ? null : result;
  }

  
  public SparkDecimal remainder(SparkDecimal divisor) {
    SparkDecimal result = new SparkDecimal();
    return !this.fastRemainder(divisor, result) ? null : result;
  }

  
  public static SparkDecimal enforcePrecisionScale(SparkDecimal dec, int maxPrecision, int maxScale) {
    if (maxPrecision >= 1 && maxPrecision <= 38) {
      if (maxScale >= 0 && maxScale <= 38) {
        if (maxPrecision < maxScale) {
          throw new IllegalArgumentException("Decimal scale must be less than or equal to precision");
        } else if (dec == null) {
          return null;
        } else {
          FastCheckPrecisionScaleStatus status = dec.fastCheckPrecisionScale(maxPrecision, maxScale);
          switch(status) {
            case NO_CHANGE:
              return dec;
            case OVERFLOW:
              return null;
            case UPDATE_SCALE_DOWN:
              SparkDecimal result = new SparkDecimal();
              if (!dec.fastUpdatePrecisionScale(maxPrecision, maxScale, status, result)) {
                return null;
              }

              return result;
            default:
              throw new RuntimeException("Unknown fast decimal check precision and scale status " + status);
          }
        }
      } else {
        throw new IllegalArgumentException("Decimal scale out of allowed range [0,38]");
      }
    } else {
      throw new IllegalArgumentException("Decimal precision out of allowed range [1,38]");
    }
  }

  
  public void validate() {
    if (!this.fastIsValid()) {
      this.fastRaiseInvalidException();
    }

  }
}
