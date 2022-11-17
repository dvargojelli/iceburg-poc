package com.dv.ice;

import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import lombok.extern.log4j.Log4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;

@Log4j
public class Minio {
    static final int MINIO_DEFAULT_PORT = 9000;
    static final String DEFAULT_IMAGE = "minio/minio";
    static final String DEFAULT_TAG = "edge";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    static final String TEST_BUCKET = "testbucket";
    public static final String MINIO_ACCESS_KEY = "minio";
    public static final String MINIO_SECRET_KEY = "minio123";
    public GenericContainer<?> container;
    public MinioClient client;

    public Minio() {
        this.container = new GenericContainer<>(DEFAULT_IMAGE )
                .waitingFor(new HttpWaitStrategy()
                        .forPath(HEALTH_ENDPOINT)
                        .forPort(MINIO_DEFAULT_PORT)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withExposedPorts(MINIO_DEFAULT_PORT)
                .withEnv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
    }

    public void start() throws NoSuchAlgorithmException, KeyManagementException, ServerException, InsufficientDataException, ErrorResponseException, IOException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {

        this.container.start();
        this.container.waitingFor(Wait.forHttp("/minio/health/ready"));

        client = MinioClient.builder()
                .endpoint("http://" + container.getHost() + ":" + container.getMappedPort(MINIO_DEFAULT_PORT))
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();
        client.ignoreCertCheck();

        client.makeBucket(MakeBucketArgs.builder()
                .bucket(TEST_BUCKET)
                .build());

        log.info("com.dv.ice.Minio Started!");
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public Integer getMappedPort() {
        return this.container.getMappedPort(MINIO_DEFAULT_PORT);
    }

    public void listBuckets()
            throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException,
            XmlParserException, ErrorResponseException {
        List<Bucket> bucketList = client.listBuckets();
        for (Bucket bucket : bucketList) {
            log.info("Bucket: " + bucket.name() + " " + bucket.creationDate());
        }
    }

    public void listFiles() {
        listFiles(null);
    }

    public void listFiles(String message) {
        log.info("-----------------------------------------------------------------");
        if (message != null) {
            log.info( message);
        }
        try {
            List<Bucket> bucketList = client.listBuckets();
            for (Bucket bucket : bucketList) {
                log.info("Bucket: " + bucket.name() + " ROOT");
                Iterable<Result<Item>> results =
                        client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
                for (Result<Item> result : results) {
                    Item item = result.get();
                    log.info("Bucket: " + bucket.name() + " Item: " + item.objectName() + " Size: " + item.size());
                }
            }
        }
        catch (Exception e) {
            log.info("Failed listing bucket");
        }
        log.info("-----------------------------------------------------------------");

    }

}
