package com.jelli.iceberg.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.util.Map;

@Configuration
public class IcebergConfig{



    @Bean
    public Catalog createJdbcCatalog(@Value("${dsr.iceberg.warehouseLocation}") final String warehouseLocation,
                                     @Value("${dsr.iceberg.catalogName}")final String catalogName,
                                     @Value("${dsr.iceberg.databaseUser}")final String databaseUser,
                                     @Value("${dsr.iceberg.databasePassword}")final String databasePassword,
                                     @Value("${dsr.iceberg.jdbcUrl}")final String jdbcUrl) throws JsonProcessingException {
        Map<String, String> conf =
                createS3JdbcIcebergConfig(warehouseLocation, jdbcUrl, databaseUser, databasePassword);
        System.out.println("Iceberg Config: " + new ObjectMapper().writeValueAsString(conf));
        JdbcCatalog newCatalog = new JdbcCatalog();
        newCatalog.initialize(catalogName, conf);
        return newCatalog;
    }


    private Map<String, String> createS3JdbcIcebergConfig(
            String s3WarehouseLocation,
            String jdbcConnectionString,
            String jdbcUser,
            String jdbcPassword
    ){
        return Map.of(
                CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName(),
                CatalogProperties.WAREHOUSE_LOCATION, s3WarehouseLocation,
                CatalogProperties.URI, jdbcConnectionString,
                JdbcCatalog.PROPERTY_PREFIX + "user", jdbcUser,
                JdbcCatalog.PROPERTY_PREFIX + "password", jdbcPassword,
                CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    }
}