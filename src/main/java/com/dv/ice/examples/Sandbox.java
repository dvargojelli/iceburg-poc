package com.dv.ice.examples;

import com.dv.ice.IcebergException;
import com.dv.ice.Setup;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class Sandbox extends Setup {

    private static final String INPUT_STRING = "someInputString";
    private Schema schema;
    private PartitionSpec partitionSpec;
    private TableIdentifier tableIdentifier;

    private Catalog catalog;

    private Table table;

    public Sandbox() throws IcebergException {
        super();
    }

    public void run(){
        init();
        Map<String, String> configuration = createConfig();
        catalog = createCatalog(INPUT_STRING, configuration);
        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
        } else {
            table = catalog.createTable(tableIdentifier, schema, partitionSpec);
        }

        doSomething();
    }

    private void doSomething(){
        System.out.println("In do something");
    }

    private Map<String, String> createConfig() {
        return Map.of(
                CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName(),
                CatalogProperties.WAREHOUSE_LOCATION, Setup.WAREHOUSE_PATH
        );
    }

    private Catalog createCatalog(String inputName, Map<String, String> conf) {
        Catalog newCatalog = new org.apache.iceberg.aws.glue.GlueCatalog();
        newCatalog.initialize(inputName, conf);
        return newCatalog;
    }

    private void init() {
        schema = new Schema(
                required(1, "hotel_id", Types.LongType.get()),
                optional(2, "hotel_name", Types.StringType.get()),
                required(3, "customer_id", Types.LongType.get()),
                required(4, "arrival_date", Types.DateType.get()),
                required(5, "departure_date", Types.DateType.get())
        );
        partitionSpec = PartitionSpec.builderFor(schema)
                .identity("hotel_id")
                .build();
        tableIdentifier = TableIdentifier.parse("bookings.rome_hotels");
    }
}
