package com.jelli.iceberg.resource;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.service.delegates.IcebergQuery;
import com.jelli.iceberg.service.delegates.IcebergReader;
import com.jelli.iceberg.service.delegates.IcebergTableOperations;
import com.jelli.iceberg.service.delegates.IcebergWriter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;


@RestController()
@RequestMapping("/iceberg")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class IcebergResource {

    private final IcebergWriter<Dataset<Row>> icebergWriter;
    private final IcebergReader<Dataset<Row>> icebergReader;
    private final IcebergQuery<Dataset<Row>> icebergQuery;
    private final IcebergTableOperations icebergTableOperations;

    @PostMapping("/upload")
    public ResponseEntity<String> insertCsvFile(
            @RequestParam("tableName") String tableName,
            @RequestParam("file") MultipartFile file) throws IOException, NoSuchTableException {
        IcebergIceTable table = icebergTableOperations.getTable(tableName);
        Dataset<Row> data = icebergWriter.write(table, file.getInputStream(), true);
        return ResponseEntity.ok(data.toString());
    }

    @GetMapping("/")
    public ResponseEntity<String> query(
            @RequestParam("query") String query) {
        Dataset<Row> data = icebergQuery.sql(URLDecoder.decode(query, Charset.defaultCharset()));
        return ResponseEntity.ok(data.toString());
    }

}
