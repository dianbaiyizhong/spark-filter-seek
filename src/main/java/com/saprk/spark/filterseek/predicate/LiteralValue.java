package com.saprk.spark.filterseek.predicate;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

/**
 * A literal in a seek predicate, normalised to the type of the column it is compared with.
 * All numeric literals become {@link BigDecimal}, so range comparison with ORC/Parquet
 * statistics never suffers from int/long/double overflow or precision surprises.
 */
public final class LiteralValue implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Object value;

  private LiteralValue(Object value) {
    this.value = value;
  }

  public Object value() {
    return value;
  }

  public static LiteralValue of(Object raw, DataType dataType) {
    if (raw == null) {
      return new LiteralValue(null);
    }
    if (raw instanceof LiteralValue) {
      return (LiteralValue) raw;
    }

    Object normalized;
    if (dataType == null) {
      normalized = normalizeByInstance(raw);
    } else if (dataType.equals(DataTypes.StringType)) {
      normalized = raw.toString();
    } else if (dataType.equals(DataTypes.BooleanType)) {
      normalized = raw instanceof Boolean ? raw : Boolean.parseBoolean(raw.toString());
    } else if (dataType.equals(DataTypes.DateType)) {
      normalized = toDays(raw);
    } else if (dataType.equals(DataTypes.TimestampType)) {
      normalized = toMicros(raw);
    } else if (dataType instanceof DecimalType || dataType.equals(DataTypes.DoubleType)
        || dataType.equals(DataTypes.FloatType)) {
      normalized = decimal(raw);
    } else if (dataType.equals(DataTypes.ByteType) || dataType.equals(DataTypes.ShortType)
        || dataType.equals(DataTypes.IntegerType) || dataType.equals(DataTypes.LongType)) {
      normalized = decimal(raw);
    } else {
      normalized = normalizeByInstance(raw);
    }
    return new LiteralValue(normalized);
  }

  public static LiteralValue of(String raw, DataType dataType) {
    return of((Object) raw, dataType);
  }

  private static Object normalizeByInstance(Object raw) {
    if (raw instanceof Number) {
      return decimal(raw);
    }
    if (raw instanceof Date) {
      return toDays(raw);
    }
    if (raw instanceof Timestamp) {
      return toMicros(raw);
    }
    return raw;
  }

  private static BigDecimal decimal(Object raw) {
    if (raw instanceof BigDecimal) {
      return (BigDecimal) raw;
    }
    if (raw instanceof Number) {
      return new BigDecimal(raw.toString());
    }
    String s = raw.toString().trim();
    if (s.isEmpty()) {
      return BigDecimal.ZERO;
    }
    return new BigDecimal(s);
  }

  private static BigDecimal toDays(Object raw) {
    if (raw instanceof Number) {
      return decimal(raw);
    }
    if (raw instanceof Date) {
      return BigDecimal.valueOf(((Date) raw).toLocalDate().toEpochDay());
    }
    if (raw instanceof LocalDate) {
      return BigDecimal.valueOf(((LocalDate) raw).toEpochDay());
    }
    String s = raw.toString().trim();
    if (s.matches("-?\\d+")) {
      return decimal(s);
    }
    return BigDecimal.valueOf(LocalDate.parse(s).toEpochDay());
  }

  private static BigDecimal toMicros(Object raw) {
    if (raw instanceof Number) {
      return decimal(raw);
    }
    if (raw instanceof Timestamp) {
      Timestamp ts = (Timestamp) raw;
      return BigDecimal.valueOf(ts.getTime() * 1000L + ts.getNanos() / 1000L);
    }
    if (raw instanceof LocalDateTime) {
      return BigDecimal.valueOf(((LocalDateTime) raw).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
    }
    String s = raw.toString().trim();
    if (s.matches("-?\\d+")) {
      return decimal(s);
    }
    return BigDecimal.valueOf(LocalDateTime.parse(s.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
  }

  /** Comparable view of a value produced by a column reader. */
  public static Comparable<?> comparable(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number && !(value instanceof BigDecimal)) {
      return new BigDecimal(value.toString());
    }
    if (value instanceof Date) {
      return BigDecimal.valueOf(((Date) value).toLocalDate().toEpochDay());
    }
    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      return BigDecimal.valueOf(ts.getTime() * 1000L + ts.getNanos() / 1000L);
    }
    if (value instanceof CharSequence) {
      return value.toString();
    }
    if (value instanceof Comparable) {
      return (Comparable<?>) value;
    }
    return value.toString().toLowerCase(Locale.ROOT);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int compare(Object left, Object right) {
    Comparable a = comparable(left);
    Comparable b = comparable(right);
    if (a == null || b == null) {
      // SQL NULL never satisfies a comparison; callers must handle null before calling compare.
      return a == b ? 0 : (a == null ? -1 : 1);
    }
    if (a.getClass().equals(b.getClass())) {
      return a.compareTo(b);
    }
    return a.toString().compareTo(b.toString());
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
