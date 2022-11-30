package com.jelli.iceberg;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.model.tables.impl.SimpleAudienceDataTable;
import com.jelli.iceberg.service.delegates.IcebergQuery;
import com.jelli.iceberg.service.delegates.IcebergTableOperations;
import com.jelli.iceberg.service.delegates.IcebergWriter;
import com.jelli.iceberg.service.impl.spark.IcebergQuerySparkImpl;
import com.jelli.iceberg.service.impl.spark.IcebergTableOperationsSparkImpl;
import com.jelli.iceberg.service.impl.spark.IcebergWriterSparkImpl;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Log4j
public class PostApplicationStartup {


    private static final SimpleAudienceDataTable SIMPLE_AUDIENCE_DATA_TABLE = new SimpleAudienceDataTable();
    private static final List<String> DEMO_FILES = List.of(
            "/Users/dvargo/Desktop/ice-poc-3/iceberg-poc/src/main/resources/csv/data_1.csv",
            "/Users/dvargo/Desktop/ice-poc-3/iceberg-poc/src/main/resources/csv/data_2.csv"
    );


    public static void initApplication(ConfigurableApplicationContext applicationContext) throws NoSuchTableException, IOException {
        IcebergWriter<Dataset<Row>> icebergWriter = applicationContext.getBean(IcebergWriterSparkImpl.class);
        IcebergQuery<Dataset<Row>> icebergQuery = applicationContext.getBean(IcebergQuerySparkImpl.class);
        IcebergTableOperations icebergTableOperations = applicationContext.getBean(IcebergTableOperationsSparkImpl.class);
        printInfo(icebergTableOperations);
        log.info("Getting or creating table");
        IcebergIceTable table = icebergTableOperations.getOrCreateTable(new SimpleAudienceDataTable());
        if(!icebergTableOperations.isEmpty(table)){
            log.info("Tables already have test data");
            return;
        }
        log.info("About to insert demo data");
        insertDemoData(icebergWriter, icebergQuery, table);
        log.info("Done with setup and inserting data");
    }

    private static void printInfo(IcebergTableOperations icebergTableOperations){
        log.info("All tables for Catalog: " + icebergTableOperations.getCatalog().name() + " and namespace: " + SIMPLE_AUDIENCE_DATA_TABLE.getTableIdentifier().namespace().toString());
        log.info(icebergTableOperations.listTables(SIMPLE_AUDIENCE_DATA_TABLE.getTableIdentifier()).stream().map(x -> "Table: " + x.name())
                .collect(Collectors.joining("\n")));
    }
    private static void insertDemoData(
            IcebergWriter<Dataset<Row>> icebergWriter,
            IcebergQuery<Dataset<Row>> icebergQuery,
            IcebergIceTable table) throws NoSuchTableException, IOException {
        for(String filePath : DEMO_FILES){
            log.info("Writing: " + filePath + " to database");
            icebergWriter.write(table, new File(filePath), false);
            Dataset<Row> dataset = icebergQuery.sql(SimpleAudienceDataTable.select().all());
            log.info("Total rows: " + dataset.count());
        }
    }

}
