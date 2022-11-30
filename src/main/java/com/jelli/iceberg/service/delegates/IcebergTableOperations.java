package com.jelli.iceberg.service.delegates;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public interface IcebergTableOperations {

    Logger log = LoggerFactory.getLogger(IcebergTableOperations.class);

    boolean isEmpty(IcebergIceTable table);
    Catalog getCatalog();

    default IcebergIceTable getTable(String tableName) throws NoSuchTableException {
        return getTable(TableIdentifier.parse(tableName));
    }
    IcebergIceTable getTable(TableIdentifier tableIdentifier) throws NoSuchTableException;
    default IcebergIceTable createTable(TableIdentifier tableIdentifier, Schema schema){
        return createTable(tableIdentifier, schema, null);
    }

    IcebergIceTable createTable(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec);

    default IcebergIceTable createTable(IcebergIceTable icebergIceTable){
        return createTable(icebergIceTable.getTableIdentifier(), icebergIceTable.getSchema(), icebergIceTable.getPartitionSpec());
    }

    default boolean tableExists(String tableId){
        return StringUtils.isNotEmpty(tableId) && getCatalog().tableExists(TableIdentifier.parse(tableId));
    }

    default boolean tableExists(IcebergIceTable icebergIceTable){
        return Objects.nonNull(icebergIceTable.getTable()) && getCatalog().tableExists(icebergIceTable.getTableIdentifier());
    }

    default boolean tableExists(TableIdentifier tableIdentifier) {
        return Objects.nonNull(tableIdentifier) && getCatalog().tableExists(tableIdentifier);
    }

    default IcebergIceTable getOrCreateTable(IcebergIceTable icebergIceTable) throws NoSuchTableException {
        return getOrCreateTable(icebergIceTable.getTableIdentifier(), icebergIceTable.getSchema());
    }

    default IcebergIceTable getOrCreateTable(TableIdentifier tableIdentifier, Schema schema) throws NoSuchTableException {
        if (getCatalog().tableExists(tableIdentifier)) {
            log.info("Getting table " + tableIdentifier.name());
            return getTable(tableIdentifier);
        } else {
            log.info("Creating table " + tableIdentifier.name());
            return createTable(tableIdentifier, schema);
        }
    }

    default IcebergIceTable getOrCreateTable(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec) throws NoSuchTableException {
        if (getCatalog().tableExists(tableIdentifier)) {
            return getTable(tableIdentifier);
        } else {
            return createTable(tableIdentifier, schema, partitionSpec);
        }
    }

    default List<TableIdentifier> listTables(TableIdentifier tableIdentifier){
        return getCatalog().listTables(tableIdentifier.namespace());
    }
}
