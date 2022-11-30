package com.jelli.iceberg;


import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

import java.io.IOException;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@Log4j
@SpringBootApplication
public class IcebergPocApplication {

    public static void main(String[] args) throws NoSuchTableException, IOException {
        ConfigurableApplicationContext applicationContext =
                SpringApplication.run(IcebergPocApplication.class, args);
        PostApplicationStartup.initApplication(applicationContext);
    }
}
