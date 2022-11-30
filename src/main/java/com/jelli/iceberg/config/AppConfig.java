package com.jelli.iceberg.config;

import com.jelli.iceberg.service.delegates.IcebergQuery;
import com.jelli.iceberg.service.delegates.IcebergReader;
import com.jelli.iceberg.service.delegates.IcebergTableOperations;
import com.jelli.iceberg.service.delegates.IcebergWriter;
import com.jelli.iceberg.service.impl.spark.IcebergQuerySparkImpl;
import com.jelli.iceberg.service.impl.spark.IcebergReaderSparkImpl;
import com.jelli.iceberg.service.impl.spark.IcebergTableOperationsSparkImpl;
import com.jelli.iceberg.service.impl.spark.IcebergWriterSparkImpl;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class AppConfig {

    private final SparkSession sparkSession; //created in spark config
    private final Catalog catalog; //created in iceberg config

    @Primary
    @Bean
    public IcebergWriter<Dataset<Row>> getIcebergWriter(IcebergReader<Dataset<Row>> icebergReader){
        return new IcebergWriterSparkImpl(sparkSession, icebergReader);
    }

    @Primary
    @Bean
    public IcebergQuery<Dataset<Row>> getIcebergQuery(){
        return new IcebergQuerySparkImpl(sparkSession);
    }

    @Primary
    @Bean
    public IcebergReader<Dataset<Row>> getIcebergReader(){
        return new IcebergReaderSparkImpl(sparkSession);
    }

    @Primary
    @Bean
    public IcebergTableOperations getIcebergTableOperations(IcebergQuery<Dataset<Row>> icebergQuery){
        return new IcebergTableOperationsSparkImpl(catalog, icebergQuery);
    }

}
