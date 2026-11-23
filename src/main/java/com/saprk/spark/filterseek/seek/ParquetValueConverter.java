package com.saprk.spark.filterseek.seek;

import java.nio.charset.StandardCharsets;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Materialises the current value of a Parquet column reader. Unlike calling getInteger/getBinary
 * directly, this also works for dictionary-encoded pages: the dictionary is installed on the
 * converter and dictionary ids are decoded exactly once per column chunk.
 */
public final class ParquetValueConverter extends PrimitiveConverter {

  public enum Kind {
    BINARY, INT, LONG, FLOAT, DOUBLE, BOOLEAN
  }

  private final Kind kind;
  private Dictionary dictionary;
  private Object value;

  public ParquetValueConverter(Kind kind) {
    this.kind = kind;
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public void addValueFromDictionary(int id) {
    if (dictionary == null) {
      value = null;
      return;
    }
    switch (kind) {
      case INT:
        value = dictionary.decodeToInt(id);
        break;
      case LONG:
        value = dictionary.decodeToLong(id);
        break;
      case FLOAT:
        value = dictionary.decodeToFloat(id);
        break;
      case DOUBLE:
        value = dictionary.decodeToDouble(id);
        break;
      case BOOLEAN:
        value = dictionary.decodeToBoolean(id);
        break;
      default:
        Binary binary = dictionary.decodeToBinary(id);
        value = binary == null ? null : new String(binary.getBytes(), StandardCharsets.UTF_8);
        break;
    }
  }

  @Override
  public void addBinary(Binary value) {
    this.value = value == null ? null : new String(value.getBytes(), StandardCharsets.UTF_8);
  }

  @Override
  public void addBoolean(boolean value) {
    this.value = value;
  }

  @Override
  public void addInt(int value) {
    this.value = value;
  }

  @Override
  public void addLong(long value) {
    this.value = value;
  }

  @Override
  public void addFloat(float value) {
    this.value = value;
  }

  @Override
  public void addDouble(double value) {
    this.value = value;
  }

  public Object value() {
    return value;
  }

  public static Kind kindOf(org.apache.spark.sql.types.DataType type) {
    if (type == null) {
      return Kind.BINARY;
    }
    if (type.equals(org.apache.spark.sql.types.DataTypes.BooleanType)) {
      return Kind.BOOLEAN;
    }
    if (type.equals(org.apache.spark.sql.types.DataTypes.IntegerType)
        || type.equals(org.apache.spark.sql.types.DataTypes.DateType)) {
      return Kind.INT;
    }
    if (type.equals(org.apache.spark.sql.types.DataTypes.LongType)
        || type.equals(org.apache.spark.sql.types.DataTypes.TimestampType)) {
      return Kind.LONG;
    }
    if (type.equals(org.apache.spark.sql.types.DataTypes.FloatType)) {
      return Kind.FLOAT;
    }
    if (type.equals(org.apache.spark.sql.types.DataTypes.DoubleType)) {
      return Kind.DOUBLE;
    }
    return Kind.BINARY;
  }
}
