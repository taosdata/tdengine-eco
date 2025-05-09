package com.taosdata.java;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;

import java.sql.Types;
import java.util.Optional;

import scala.jdk.javaapi.OptionConverters;

import java.sql.Types;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import scala.Option;
import scala.Some;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;


public class TDengineDialect extends JdbcDialect {

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:TAOS");
    }

    @Override
    public String getTableExistsQuery(String table) {
        return "SELECT COUNT(*) FROM information_schema.ins_tables WHERE table_name = '" + table + "'";
    }

    /* 
    @Override
    public String getSchemaQuery(String table) {
        //return "select col_name as COLUMN_NAME, col_type as DATA_TYPE from information_schema.ins_columns where table_name = '" + table + "'";
        return "select * from  " + table + " limit 0 ";
    }
    */

    @Override
    public String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        // 将 Spark SQL 数据类型映射到 TDengine 数据类型
        if (dt instanceof TimestampType) {
            return Option.apply(new JdbcType("TIMESTAMP", Types.TIMESTAMP));
        } else if (dt instanceof FloatType) {
            return Option.apply(new JdbcType("FLOAT", Types.FLOAT));
        } else if (dt instanceof DoubleType) {
            return Option.apply(new JdbcType("DOUBLE", Types.DOUBLE));
        } else if (dt instanceof IntegerType) {
            return Option.apply(new JdbcType("INT", Types.INTEGER));
        } else if (dt instanceof LongType) {
            return Option.apply(new JdbcType("BIGINT", Types.BIGINT));
        } else if (dt instanceof ShortType) {
            return Option.apply(new JdbcType("SMALLINT", Types.SMALLINT));
        } else if (dt instanceof ByteType) {
            return Option.apply(new JdbcType("TINYINT", Types.TINYINT));
        } else if (dt instanceof BooleanType) {
            return Option.apply(new JdbcType("BOOL", Types.BOOLEAN));
        } else if (dt instanceof StringType) {
            // TDengine 中 VARCHAR 需要指定长度，根据表定义调整
            return Option.apply(new JdbcType("VARCHAR(255)", Types.VARCHAR));
        } else if (dt instanceof BinaryType) {
            return Option.apply(new JdbcType("BINARY(255)", Types.VARBINARY));
        }
        return Option.empty();
    } 
}

