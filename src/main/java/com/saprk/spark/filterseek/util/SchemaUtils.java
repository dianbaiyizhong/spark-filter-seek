package com.saprk.spark.filterseek.util;

import java.util.List;
import java.util.Locale;

import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Schema mapping between Spark, ORC and Parquet. Only top-level primitive fields are supported. */
public final class SchemaUtils {

  private SchemaUtils() {
  }

  public static StructType orcToSpark(TypeDescription schema) {
    if (schema == null || schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("ORC root schema must be a struct");
    }
    List<String> names = schema.getFieldNames();
    List<TypeDescription> children = schema.getChildren();
    StructField[] fields = new StructField[names.size()];
    for (int i = 0; i < names.size(); i++) {
      fields[i] = DataTypes.createStructField(names.get(i), orcType(children.get(i)), true);
    }
    return DataTypes.createStructType(fields);
  }

  private static DataType orcType(TypeDescription type) {
    switch (type.getCategory()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case BYTE:
        return DataTypes.ByteType;
      case SHORT:
        return DataTypes.ShortType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case STRING:
      case VARCHAR:
      case CHAR:
        return DataTypes.StringType;
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return DataTypes.TimestampType;
      case DECIMAL:
        return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
      case BINARY:
        return DataTypes.BinaryType;
      default:
        throw new IllegalArgumentException(
            "Unsupported ORC type '" + type.getCategory() + "' for seek scan (top-level primitive only)");
    }
  }

  public static StructType parquetToSpark(MessageType schema) {
    StructField[] fields = new StructField[schema.getFieldCount()];
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type field = schema.getType(i);
      if (!field.isPrimitive()) {
        throw new IllegalArgumentException(
            "Unsupported Parquet nested field '" + field.getName() + "' for seek scan");
      }
      fields[i] = DataTypes.createStructField(field.getName(),
          parquetType((PrimitiveType) field), true);
    }
    return DataTypes.createStructType(fields);
  }

  private static DataType parquetType(PrimitiveType type) {
    PrimitiveType.PrimitiveTypeName name = type.getPrimitiveTypeName();
    OriginalType original = type.getOriginalType();
    switch (name) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT32:
        return original == OriginalType.DATE ? DataTypes.DateType : DataTypes.IntegerType;
      case INT64:
        return original == OriginalType.TIMESTAMP_MILLIS || original == OriginalType.TIMESTAMP_MICROS
            ? DataTypes.TimestampType : DataTypes.LongType;
      case INT96:
        throw new IllegalArgumentException("Parquet INT96 timestamp is not supported by seek scan");
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BINARY:
        return original == OriginalType.UTF8 ? DataTypes.StringType : DataTypes.BinaryType;
      case FIXED_LEN_BYTE_ARRAY:
        if (original == OriginalType.DECIMAL) {
          return DataTypes.createDecimalType(type.getDecimalMetadata().getPrecision(),
              type.getDecimalMetadata().getScale());
        }
        return DataTypes.BinaryType;
      default:
        throw new IllegalArgumentException("Unsupported Parquet type: " + name);
    }
  }

  public static int indexOf(StructType schema, String column) {
    String lower = column.toLowerCase(Locale.ROOT);
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().toLowerCase(Locale.ROOT).equals(lower)) {
        return i;
      }
    }
    return -1;
  }

  public static DataType typeOf(StructType schema, String column) {
    int index = indexOf(schema, column);
    return index < 0 ? null : schema.fields()[index].dataType();
  }
}
