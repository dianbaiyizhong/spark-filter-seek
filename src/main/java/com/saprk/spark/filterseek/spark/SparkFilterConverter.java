package com.saprk.spark.filterseek.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.LiteralValue;
import com.saprk.spark.filterseek.predicate.SeekPredicate;

/** Converts the filters pushed by Spark into the same predicate tree used by Druid SQL parsing. */
public final class SparkFilterConverter {

  private SparkFilterConverter() {
  }

  public static final class Result {
    public final SeekPredicate predicate;
    public final Filter[] accepted;
    public final Filter[] remaining;

    Result(SeekPredicate predicate, Filter[] accepted, Filter[] remaining) {
      this.predicate = predicate;
      this.accepted = accepted;
      this.remaining = remaining;
    }
  }

  public static Result convert(Filter[] filters, StructType schema) {
    List<SeekPredicate> predicates = new ArrayList<>();
    List<Filter> accepted = new ArrayList<>();
    List<Filter> remaining = new ArrayList<>();
    if (filters != null) {
      for (Filter filter : filters) {
        collect(filter, schema, predicates, accepted, remaining);
      }
    }
    return new Result(SeekPredicate.and(predicates.toArray(new SeekPredicate[0])),
        accepted.toArray(new Filter[0]), remaining.toArray(new Filter[0]));
  }

  /** AND nodes are split so that every supported leaf can be pushed down. */
  private static void collect(Filter filter, StructType schema, List<SeekPredicate> predicates,
      List<Filter> accepted, List<Filter> remaining) {
    if (filter instanceof And) {
      collect(((And) filter).left(), schema, predicates, accepted, remaining);
      collect(((And) filter).right(), schema, predicates, accepted, remaining);
      return;
    }
    SeekPredicate predicate = single(filter, schema);
    if (predicate == null) {
      remaining.add(filter);
    } else {
      predicates.add(predicate);
      accepted.add(filter);
    }
  }

  private static SeekPredicate single(Filter filter, StructType schema) {
    if (filter == null) {
      return null;
    }
    if (filter instanceof EqualTo) {
      return comparison(schema, filter, SeekPredicate.Op.EQ, ((EqualTo) filter).value());
    }
    if (filter instanceof GreaterThan) {
      return comparison(schema, filter, SeekPredicate.Op.GT, ((GreaterThan) filter).value());
    }
    if (filter instanceof GreaterThanOrEqual) {
      return comparison(schema, filter, SeekPredicate.Op.GTE,
          ((GreaterThanOrEqual) filter).value());
    }
    if (filter instanceof LessThan) {
      return comparison(schema, filter, SeekPredicate.Op.LT, ((LessThan) filter).value());
    }
    if (filter instanceof LessThanOrEqual) {
      return comparison(schema, filter, SeekPredicate.Op.LTE, ((LessThanOrEqual) filter).value());
    }
    if (filter instanceof In) {
      In in = (In) filter;
      String column = attribute(filter);
      DataType dataType = dataType(schema, column);
      if (dataType == null) {
        return null;
      }
      List<LiteralValue> values = new ArrayList<>();
      for (Object value : in.values()) {
        values.add(LiteralValue.of(value, dataType));
      }
      return new SeekPredicate.In(column, values, false);
    }
    if (filter instanceof StringContains) {
      return like(schema, filter, "%" + escapeLike(((StringContains) filter).value()) + "%");
    }
    if (filter instanceof StringStartsWith) {
      return like(schema, filter, escapeLike(((StringStartsWith) filter).value()) + "%");
    }
    if (filter instanceof StringEndsWith) {
      return like(schema, filter, "%" + escapeLike(((StringEndsWith) filter).value()));
    }
    if (filter instanceof Or) {
      SeekPredicate left = single(((Or) filter).left(), schema);
      SeekPredicate right = single(((Or) filter).right(), schema);
      if (left == null || right == null) {
        return null;
      }
      return new SeekPredicate.Or(left, right);
    }
    if (filter instanceof Not) {
      SeekPredicate child = single(((Not) filter).child(), schema);
      return child == null ? null : new SeekPredicate.Not(child);
    }
    return null;
  }

  private static SeekPredicate comparison(StructType schema, Filter filter,
      SeekPredicate.Op op, Object value) {
    String column = attribute(filter);
    DataType dataType = dataType(schema, column);
    if (dataType == null) {
      return null;
    }
    return new SeekPredicate.Comparison(column, op, LiteralValue.of(value, dataType));
  }

  private static SeekPredicate like(StructType schema, Filter filter, String pattern) {
    String column = attribute(filter);
    return dataType(schema, column) == null ? null : new SeekPredicate.Like(column, pattern, false);
  }

  private static String attribute(Filter filter) {
    String[] references = filter.references();
    if (references == null || references.length == 0) {
      return null;
    }
    return references[0].toLowerCase(Locale.ROOT);
  }

  private static DataType dataType(StructType schema, String column) {
    if (schema == null || column == null) {
      return null;
    }
    for (StructField field : schema.fields()) {
      if (field.name().equalsIgnoreCase(column)) {
        return field.dataType();
      }
    }
    return null;
  }

  private static String escapeLike(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }
}
