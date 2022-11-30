package com.jelli.iceberg.model.tables;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@Component
@AllArgsConstructor
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class IcebergIceTable {

    @Setter(AccessLevel.PROTECTED)
    private Schema schema;
    @Setter(AccessLevel.PROTECTED)
    private PartitionSpec partitionSpec;
    @Setter(AccessLevel.PROTECTED)
    private TableIdentifier tableIdentifier;
    @Setter
    private Table table;

    public IcebergIceTable(Schema schema,PartitionSpec partitionSpec, TableIdentifier tableIdentifier){
        this.schema = schema;
        this.partitionSpec = partitionSpec;
        this.tableIdentifier = tableIdentifier;
    }

    public IcebergIceTable(Table table, TableIdentifier tableIdentifier) {
        this.schema = table.schema();
        this.partitionSpec = table.spec();
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public static IcebergIceTable of(Table table, TableIdentifier tableIdentifier){
        return new IcebergIceTable(table, tableIdentifier) {
            @Override
            public Schema getSchema() {
                return super.getSchema();
            }
        };
    }

}
