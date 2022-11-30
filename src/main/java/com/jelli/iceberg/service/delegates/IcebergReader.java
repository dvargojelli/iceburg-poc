package com.jelli.iceberg.service.delegates;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import org.apache.iceberg.Schema;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IcebergReader<T> {

    Logger log = LoggerFactory.getLogger(IcebergReader.class);
    SparkSession getSparkSession();

    default T readFile(IcebergIceTable icebergIceTable, String csvFilePath){
        return readFile(icebergIceTable.getSchema(), csvFilePath);
    }

    T readFile(Schema schema, String csvFilePath);


}
