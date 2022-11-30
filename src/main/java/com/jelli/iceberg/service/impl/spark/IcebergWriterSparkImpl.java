package com.jelli.iceberg.service.impl.spark;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.service.delegates.IcebergReader;
import com.jelli.iceberg.service.delegates.IcebergWriter;
import com.jelli.iceberg.utils.DiskUtils;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;


@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class IcebergWriterSparkImpl implements IcebergWriter<Dataset<Row>> {

    private final SparkSession sparkSession;
    private final IcebergReader<Dataset<Row>> icebergReader;

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public Dataset<Row> write(IcebergIceTable table, File csvFile, boolean show) throws NoSuchTableException {
        Dataset<Row> dataset = icebergReader.readFile(table, csvFile.getAbsolutePath());
        if(show) {
            log.info("What was read in:");
            dataset.show();
        }
        write(table.getTable(), dataset);
        return dataset;
    }

    @Override
    public Dataset<Row> write(IcebergIceTable table, InputStream inputStream, boolean show) throws IOException, NoSuchTableException {
        if(Objects.isNull(table) || Objects.isNull(table.getTable())){
            throw new NoSuchTableException("Table not created yet, create it before putting data in");
        }
        File file = DiskUtils.writeToDisk(table.getTable().name(), inputStream);
        if(!file.exists()){
            throw new IOException("Could not write csv file to disk");
        }
        log.info("Temp file path is: " + file.getAbsolutePath());
        Dataset<Row> dataset = write(table, file, show);
        file.delete();
        return dataset;
    }


    public Dataset<Row> write(Table table, Dataset<Row> dataset) throws NoSuchTableException {
        Transaction transaction = table.newTransaction();
        try {
            dataset.writeTo(table.name()).append();
        }catch (AnalysisException e){
            log.info("Appears this table has never had data inserted, attempting to create via spark");
            dataset.writeTo(table.name()).createOrReplace();
        }
        transaction.commitTransaction();
        return dataset;
    }

}
