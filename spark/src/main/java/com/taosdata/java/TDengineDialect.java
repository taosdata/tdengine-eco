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

}

