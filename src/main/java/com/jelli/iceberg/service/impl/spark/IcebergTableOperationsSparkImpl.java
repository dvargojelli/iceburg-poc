package com.jelli.iceberg.service.impl.spark;

import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.service.delegates.IcebergQuery;
import com.jelli.iceberg.service.delegates.IcebergTableOperations;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class IcebergTableOperationsSparkImpl implements IcebergTableOperations {

    private final Catalog catalog;
    private final IcebergQuery<Dataset<Row>> icebergQuery;

    @Override
    public boolean isEmpty(IcebergIceTable table) {
        return icebergQuery.sql("select * from " + table.getTable().name(), false).isEmpty();
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public IcebergIceTable getTable(TableIdentifier tableIdentifier) throws NoSuchTableException{
        if(!tableExists(tableIdentifier)){
            throw new NoSuchTableException("Table doesnt exist");
        }
        Table table =  catalog.loadTable(tableIdentifier);
        return IcebergIceTable.of(table, tableIdentifier);
    }



    @Override
    public IcebergIceTable createTable(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec) {
        Table table;
        if(Objects.nonNull(partitionSpec)){
            table = catalog.createTable(tableIdentifier, schema, partitionSpec);
        }
        else{
            table = catalog.createTable(tableIdentifier, schema);
        }
        return IcebergIceTable.of(table, tableIdentifier);
    }
}
