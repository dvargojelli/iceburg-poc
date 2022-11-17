package com.dv.ice.examples;

import com.dv.ice.Setup;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Log4j

public class IcebergJavaApiUpsert extends Setup {


    ArrayList<Record> sampleIcebergrecords = Lists.newArrayList();
    ArrayList<Record> sampleIcebergrecordsUpsert = Lists.newArrayList();

    public IcebergJavaApiUpsert() throws Exception {
        super();

        GenericRecord record = GenericRecord.create(SparkSchemaUtil.convert(getSampleDf().schema()));
        sampleIcebergrecords.add(record.copy("age", 29L, "name", "JavaAPI User-a"));
        sampleIcebergrecords.add(record.copy("age", 43L, "name", "JavaAPI User-b"));

        sampleIcebergrecordsUpsert.add(record.copy("age", 129L, "name", "JavaAPI User-a"));
        sampleIcebergrecordsUpsert.add(record.copy("age", 123L, "name", "JavaAPI User-b"));

    }

    public void run() throws IOException, NoSuchTableException, TableAlreadyExistsException, NoSuchNamespaceException, InterruptedException {
        // get catalog from spark
        SparkSessionCatalog sparkSessionCatalog = (SparkSessionCatalog) getSpark().sessionState().catalogManager().v2SessionCatalog();
        Identifier tableIdentifier = Identifier.of(Namespace.of("default").levels(), "iceberg_table");
        Schema tableSchema = SparkSchemaUtil.convert(getSampleDf().schema());
        log.info("Iceberg Table schema is: " + tableSchema.asStruct());

        Map<String, String> options = Maps.newHashMap();
        Transform[] transforms = {};
        sparkSessionCatalog.createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
        SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);

        log.warn("------------AFTER Spark SQL INSERT----------------");
        getSpark().sql("INSERT INTO default.iceberg_table VALUES (10,'spark sql-insert')");
        getSpark().sql("select * from default.iceberg_table").show(false);
        log.warn("------------AFTER Dataframe APPEND----------------");
        getSampleDf().writeTo("default.iceberg_table").append();
        getSpark().sql("select * from default.iceberg_table").show(false);

        //********************************* JAVA API APPEND data *********************************
        FileIO outFile = sparkTable.table().io();
        OutputFile out = outFile.newOutputFile(sparkTable.table().locationProvider().newDataLocation(UUID.randomUUID() + "-001"));
        FileAppender<Record> writer = Parquet.write(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(sparkTable.table())
                .overwrite()
                .build();
        try (Closeable toClose = writer) {
            writer.addAll(sampleIcebergrecords);
        }

        DataFile appendDataFile = DataFiles.builder(sparkTable.table().spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();

        sparkTable.table().newAppend()
                .appendFile(appendDataFile)
                .commit();
        log.warn("------------AFTER JAVA API APPEND----------------");
        getSpark().sql("select * from default.iceberg_table").show(false);

        // ********************************* JAVa API UPSERT >>> DELETE + INSERT *********************************
        Table myV2IcebergTable = this.upgradeToFormatVersion2(sparkTable.table());
        // LETS USE NAME as SORT ORDER/PK
        myV2IcebergTable.replaceSortOrder().asc("name").commit();
        List<Integer> equalityDeleteFieldIds = new ArrayList<>();
        equalityDeleteFieldIds.add(myV2IcebergTable.schema().findField("name").fieldId());

        DataFile upsertDataFile = getDataFile(myV2IcebergTable, sampleIcebergrecordsUpsert);
        DeleteFile deleteDataFile = getDeleteDataFile(myV2IcebergTable, sampleIcebergrecords, equalityDeleteFieldIds);
        log.debug("Committing new file as Upsert (has deletes: " + deleteDataFile != null + ") '" + upsertDataFile.path() + "' !");
        RowDelta c = myV2IcebergTable
                .newRowDelta()
                .addDeletes(deleteDataFile)
                .validateDeletedFiles()
                .addRows(upsertDataFile);

        c.commit();
        log.info("Committed events to table! " + myV2IcebergTable.location());
        log.warn("------------AFTER JAVA API UPSERT----------------");
        getSpark().sql("select * from default.iceberg_table").show(false);

        log.warn("------------FINAL S3 FILE LIST ----------------");
        getS3().listFiles();

    }

    // @TODO remove once spec v2 released! upgrading table to V2
    public Table upgradeToFormatVersion2(Table icebergTable) {
        // Upgrade V1 table to V2 specs, V2 specs is not released yet so we are manually upgrading it
        TableOperations ops = ((BaseTable) icebergTable).operations();
        TableMetadata meta = ops.current();
        ops.commit(ops.current(), meta.upgradeToFormatVersion(2));
        icebergTable.refresh();
        return icebergTable;
    }

    private DeleteFile getDeleteDataFile(Table icebergTable, ArrayList<Record> icebergRecords, List<Integer> equalityDeleteFieldIds) throws InterruptedException {

        final String fileName = "del-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
        OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

        EqualityDeleteWriter<Record> deleteWriter;

        try {
            log.debug("Writing data to equality delete file: " + out + "!");

            deleteWriter = Parquet.writeDeletes(out)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .rowSchema(icebergTable.sortOrder().schema())
                    .withSpec(icebergTable.spec())
                    .equalityFieldIds(equalityDeleteFieldIds)
                    //.withKeyMetadata() // ??
                    .metricsConfig(MetricsConfig.fromProperties(icebergTable.properties()))
                    // .withPartition() // ??
                    // @TODO add sort order v12 ??
                    .setAll(icebergTable.properties())
                    .buildEqualityWriter()
            ;

            try (Closeable toClose = deleteWriter) {
                deleteWriter.deleteAll(icebergRecords);
            }

        } catch (IOException e) {
            throw new InterruptedException(e.getMessage());
        }

        log.debug("Creating iceberg equality delete file!");
        // Equality delete files identify deleted rows in a collection of data files by one or more column values,
        // and may optionally contain additional columns of the deleted row.
        return FileMetadata.deleteFileBuilder(icebergTable.spec())
                .ofEqualityDeletes(ArrayUtil.toIntArray(equalityDeleteFieldIds))
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(deleteWriter.length())
                //.withMetrics(deleteWriter.metrics()) //
                .withRecordCount(icebergRecords.size()) // its mandatory field! replace when with iceberg V 0.12
                //.withSortOrder(icebergTable.sortOrder())
                .build();
    }

    private DataFile getDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
        final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
        OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

        FileAppender<Record> writer;
        try {
            log.debug("Writing data to file: " + out + "!");
            writer = Parquet.write(out)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .forTable(icebergTable)
                    .overwrite()
                    .build();

            try (Closeable toClose = writer) {
                writer.addAll(icebergRecords);
            }

        } catch (IOException e) {
            throw new InterruptedException(e.getMessage());
        }

        log.debug("Creating iceberg DataFile!");
        return DataFiles.builder(icebergTable.spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();
    }


}
