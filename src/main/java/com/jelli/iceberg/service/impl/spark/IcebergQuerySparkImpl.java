package com.jelli.iceberg.service.impl.spark;

import com.jelli.iceberg.service.delegates.IcebergQuery;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class IcebergQuerySparkImpl implements IcebergQuery<Dataset<Row>> {

    private final SparkSession sparkSession;

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public Dataset<Row> sql(String sql, boolean show) {
        Dataset<Row> dataset = getSparkSession().sql(sql);
        if(show){
            dataset.show();
        }
        return dataset;
    }
}
