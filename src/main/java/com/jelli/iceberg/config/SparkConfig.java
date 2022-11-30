package com.jelli.iceberg.config;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class SparkConfig{

    private final String warehouseLocation;
    private final String catalogName;
    private final String databaseUser;
    private final String databasePassword;
    private final String jdbcUrl;

    @Autowired
    public SparkConfig(@Value("${dsr.iceberg.warehouseLocation}") final String warehouseLocation,
                       @Value("${dsr.iceberg.catalogName}")final String catalogName,
                       @Value("${dsr.iceberg.databaseUser}")final String databaseUser,
                       @Value("${dsr.iceberg.databasePassword}")final String databasePassword,
                       @Value("${dsr.iceberg.jdbcUrl}")final String jdbcUrl){
        this.warehouseLocation = warehouseLocation;
        this.catalogName = catalogName;
        this.databasePassword = databasePassword;
        this.databaseUser = databaseUser;
        this.jdbcUrl = jdbcUrl;
    }


    @Bean
    public SparkSession getSparkSession() throws JsonProcessingException {
        Map<String,String> configMap = createConfigMap();
        System.out.println("Spark Config: " + new ObjectMapper().writeValueAsString(configMap));
        SparkSession.Builder sparkSessionBuilder = SparkSession.builder()
                .master("local[*]")
                .appName("DSR_Application_" + System.currentTimeMillis());
        configMap.forEach((key,val) -> sparkSessionBuilder.config(key,val));
        return sparkSessionBuilder.getOrCreate();
    }

    private Map<String,String> createConfigMap(){
        return Map.of(
                "spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog",
                withCatalogName("spark.sql.catalog", catalogName), "org.apache.iceberg.spark.SparkCatalog",
                withCatalogName("spark.sql.catalog", catalogName, "warehouse"), warehouseLocation,
                "spark.sql.defaultCatalog",catalogName,
                withCatalogName("spark.sql.catalog", catalogName, "catalog-impl"), JdbcCatalog.class.getName(),
                withCatalogName("spark.sql.catalog", catalogName,"io-impl"), S3FileIO.class.getName(),
                withCatalogName("spark.sql.catalog", catalogName,"jdbc.user"), databaseUser,
                withCatalogName("spark.sql.catalog", catalogName,"jdbc.password"), databasePassword,
                withCatalogName("spark.sql.catalog", catalogName,"uri"), jdbcUrl);
    }

    private static String withCatalogName(String configPrefix, String catalogName){
        String result = configPrefix + "." + catalogName;
        return result;
    }

    private static String withCatalogName(String configPrefix, String catalogName, String configPostfix){
        String result = withCatalogName(configPrefix, catalogName) + "." + configPostfix;
        return result;
    }

}