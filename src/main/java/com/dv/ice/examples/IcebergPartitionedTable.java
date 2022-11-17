package com.dv.ice.examples;

import com.dv.ice.IcebergException;
import com.dv.ice.Setup;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.hadoop.HadoopTables;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Log4j
public class IcebergPartitionedTable extends Setup {

    public IcebergPartitionedTable() throws IcebergException {
        super();
    }

    public void createTable(TableIdentifier table){
        // create table
        getSpark().sql("CREATE TABLE " + table + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string ," +
                "    current boolean," +
                "    effective_date date," +
                "    end_date date" +
                ") USING iceberg " +
                " " +
                "");
    }

    public void loadData(){
        // load test data
        getSpark().sql("INSERT INTO default.partitioned_table " +
                "select 1, 'customer_a-V1', false, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('2020-01-12', 'yyyy-MM-dd');");
        getSpark().sql("INSERT INTO default.partitioned_table " +
                "select 1, 'customer_a-V2', true, to_date('2020-01-12', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");
        getSpark().sql("INSERT INTO default.partitioned_table " +
                "select 2, 'customer_b-V1', true, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");
    }

    public void run() {
        TableIdentifier table = TableIdentifier.of("default.partitioned_table");
        createTable(table);
        loadData();



        log.warn("------- TABLE -------------------------------");
        HadoopTables tables = new HadoopTables(this.getSpark().sparkContext().hadoopConfiguration());
        String tablePath = Setup.WAREHOUSE_PATH + "/default/partitioned_table";
        Table ptttable = tables.load(tablePath);
        log.info("schema: " + ptttable.schema());
        log.info("spec: " + ptttable.spec());

        GenericRecord record = GenericRecord.create(ptttable.schema().asStruct());
        record.setField("customer_id", 1);
        record.setField("name", "customer_c-V1");
        record.setField("effective_date", LocalDate.parse("2020-03-01", DateTimeFormatter.ISO_LOCAL_DATE));
        record.setField("current", false);

        PartitionKey partitionKey = new PartitionKey(ptttable.spec(), ptttable.schema());
        InternalRecordWrapper wrapper = new InternalRecordWrapper(record.struct());
        partitionKey.partition(wrapper.wrap(record));

        log.info("record: " + record);
        log.info("PartitionKey: " + partitionKey);
        log.info("newDataLocation: " + ptttable.locationProvider().newDataLocation(partitionKey.toPath()));
        log.info("newDataLocation: " + ptttable.locationProvider().newDataLocation(""));
        log.info("newDataLocation: " + ptttable.locationProvider().newDataLocation("test"));
        assert !ptttable.spec().partitionToPath(record).equals("effective_date_month=2020-03/name_trunc=customer_c") : "wrong PTT";

        log.warn("------- AFTER -------------------------------");
        getSpark().table("default.partitioned_table").orderBy("customer_id", "effective_date").show();
        log.warn("------- FINAL S3 FILES -------------------------------");
        getS3().listFiles();
    }

}