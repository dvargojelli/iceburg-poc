package com.jelli.iceberg.service.impl.spark;

import com.jelli.iceberg.service.delegates.IcebergReader;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class IcebergReaderSparkImpl implements IcebergReader<Dataset<Row>> {

    private final SparkSession sparkSession;

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public Dataset<Row> readFile(Schema schema, String csvFilePath) {
        return getSparkSession()
                .read()
                .schema(SparkSchemaUtil.convert(schema))
                .csv(csvFilePath);
    }
}
