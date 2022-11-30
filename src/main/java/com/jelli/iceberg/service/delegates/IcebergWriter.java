package com.jelli.iceberg.service.delegates;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface IcebergWriter<T> {
    Logger log = LoggerFactory.getLogger(IcebergWriter.class);
    SparkSession getSparkSession();

    T write(IcebergIceTable table, File csvFile, boolean show) throws NoSuchTableException;

    T write(IcebergIceTable table, InputStream inputStream, boolean show) throws IOException, NoSuchTableException;

}
