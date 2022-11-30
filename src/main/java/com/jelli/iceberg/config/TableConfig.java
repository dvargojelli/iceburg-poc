package com.jelli.iceberg.config;


import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.service.delegates.IcebergTableOperations;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.List;

@Log4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class TableConfig {

    private final IcebergTableOperations icebergTableOperations;
    private final List<? extends IcebergIceTable> tables;

    @PostConstruct
    public void postConstruct(){
        CollectionUtils.emptyIfNull(tables)
                .stream()
                .peek(this::logTable)
                .filter(this::tableDoesNotExists)
                .peek(this::logCreating)
                .forEach(this::createTable);
    }

    private void logCreating(IcebergIceTable icebergIceTable) {
        log.info("Creating: " + icebergIceTable.getTableIdentifier().name());
    }

    private void createTable(IcebergIceTable icebergIceTable) {
        icebergTableOperations.createTable(icebergIceTable);
    }

    private boolean tableDoesNotExists(IcebergIceTable icebergIceTable) {
        return icebergTableOperations.isEmpty(icebergIceTable);
    }

    private void logTable(IcebergIceTable icebergIceTable) {
        log.info("Found table: " + icebergIceTable.getTableIdentifier().name());
    }

}
