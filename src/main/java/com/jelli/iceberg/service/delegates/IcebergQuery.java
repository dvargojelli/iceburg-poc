package com.jelli.iceberg.service.delegates;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IcebergQuery<T> {

    Logger log = LoggerFactory.getLogger(IcebergQuery.class);
    SparkSession getSparkSession();

    default T sql(String sql){
        return sql(sql, true);
    }

    T sql(String sql, boolean show);

}
