package com.dv.ice;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Log4j
public abstract class Setup {

    public static final String WAREHOUSE_PATH = "s3a://" + Minio.TEST_BUCKET + "/my-iceberg-warehouse";
    private SparkConf sparkconf;
    private Minio s3;

    private SparkSession spark;
    private Map<String, String> icebergOptions = new ConcurrentHashMap<>();
    private Dataset<Row> sampleDf;

    public Setup() throws IcebergException {
        sparkconf = new SparkConf();
        s3 = new Minio();
        startMinio();
        setupSparkConfig();
        setupSpark();
        setupIceberg();
        createSampleDataSet();
    }



    protected void setupSparkConfig() {
        sparkconf.setAppName("CDC-S3-Batch-Spark-Sink")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.eventLog.enabled", "false")
                .set("spark.hadoop.fs.s3a.access.key", Minio.MINIO_ACCESS_KEY)
                .set("spark.hadoop.fs.s3a.secret.key", Minio.MINIO_SECRET_KEY)
                // minio specific setting using minio as S3
                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + s3.getMappedPort())
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                // enable iceberg SQL Extensions
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                .set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                .set("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE_PATH)
                .set("spark.sql.warehouse.dir", WAREHOUSE_PATH)
        ;
    }

    protected void setupSpark() {
        spark = SparkSession
                .builder()
                .config(this.sparkconf)
                .getOrCreate();

    }

    protected void setupIceberg() {
        icebergOptions.put("warehouse", WAREHOUSE_PATH);
        icebergOptions.put("type", "hadoop");
        icebergOptions.put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
    }

    protected void startMinio() throws IcebergException {
        try{
            s3.start();
        }
        catch (NoSuchAlgorithmException |
                KeyManagementException |
                ServerException |
                InsufficientDataException |
                ErrorResponseException |
                IOException |
                InvalidKeyException |
                InvalidResponseException|
                XmlParserException| InternalException e){
            throw new IcebergException("Could not start Minio", e);
        }
    }

    protected void createSampleDataSet() {

        log.warn(spark.sparkContext().getConf().toDebugString());

        List<String> jsonData = Arrays
                .asList("{'name':'User-1', 'age':1122}\n{'name':'User-2', 'age':1130}\n{'name':'User-3', 'age':1119}"
                        .split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = this.spark.createDataset(jsonData, Encoders.STRING());
        sampleDf = spark.read().json(ds).toDF();

        log.warn("Spark Version: " + spark.version());
    }

}
