package com.jelli.iceberg.model.tables.impl;

import com.jelli.iceberg.model.tables.IcebergColumn;
import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.model.tables.InjectableColumn;
import com.jelli.iceberg.model.tables.InjectableTable;
import com.jelli.iceberg.utils.Select;
import lombok.Getter;
import lombok.Setter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

@Getter
@Setter
@InjectableTable(nameSpace = SimpleAudienceDataTable.NAMESPACE, tableName = SimpleAudienceDataTable.TABLE_NAME,
        columns = {
                @InjectableColumn(id = 1, name = SimpleAudienceDataTable.COLUMN_FCCID, type = Type.TypeID.INTEGER),
                @InjectableColumn(id = 2, name = SimpleAudienceDataTable.COLUMN_CALL_LETTER)
        })
public class SimpleAudienceDataTable extends IcebergIceTable {
    public static Select select(){
        return new Select(TABLE_ID);
    }

    public static final String NAMESPACE = "civis";
    public static final String TABLE_NAME = "test_data_15";
    public static final String TABLE_ID = NAMESPACE + "." + TABLE_NAME;

    public static final String COLUMN_FCCID = "fccid";
    public static final String COLUMN_CALL_LETTER= "call_letter";
    public static final String COLUMN_MARKET_CODE = "market_code";
    public static final String COLUMN_BOOK_NAME = "book_name";
    public static final String COLUMN_EXTERNAL_BOOK_ID = "external_book_id";
    public static final String COLUMN_AUDIENCE_SEGMENT = "audience_segment";
    public static final String COLUMN_AUDIENCE_SEGMENT_ID = "external_audience_segment_id";
    public static final String COLUMN_DAYPART = "daypart";
    public static final String COLUMN_DEMO = "demo";
    public static final String COLUMN_CUME = "cume";
    public static final String COLUMN_AQH = "aqh";

    public SimpleAudienceDataTable() {
        super();
        setSchema(createSchema());
        setPartitionSpec(getPartitionSpec());
        setTableIdentifier(createTableIdentifier());
    }

    private Schema createSchema() {
        return new Schema(getColumns().stream()
                .map(IcebergColumn::toIcebergSchema)
                .collect(Collectors.toList())
                .toArray(new Types.NestedField[]{}));
    }

    private PartitionSpec createParitionSpec() {
        return null; //none for now
    }

    private TableIdentifier createTableIdentifier() {
        return TableIdentifier.parse(TABLE_ID);
    }


    public List<IcebergColumn> getColumns() {
        return List.of(
                new IcebergColumn<>(1, COLUMN_FCCID, Types.LongType.get(), true),
                new IcebergColumn<>(2, COLUMN_CALL_LETTER, Types.StringType.get(), true),
                new IcebergColumn<>(3, COLUMN_MARKET_CODE, Types.LongType.get(), true),
                new IcebergColumn<>(4, COLUMN_BOOK_NAME, Types.StringType.get(), true),
                new IcebergColumn<>(5, COLUMN_EXTERNAL_BOOK_ID, Types.LongType.get(), true),
                new IcebergColumn<>(6, COLUMN_AUDIENCE_SEGMENT, Types.StringType.get(), true),
                new IcebergColumn<>(7, COLUMN_AUDIENCE_SEGMENT_ID, Types.LongType.get(), true),
                new IcebergColumn<>(8, COLUMN_DAYPART, Types.StringType.get(), true),
                new IcebergColumn<>(9, COLUMN_DEMO, Types.StringType.get(), true),
                new IcebergColumn<>(10, COLUMN_CUME, Types.LongType.get(), true),
                new IcebergColumn<>(12, COLUMN_AQH, Types.LongType.get(), true)
        );
    }

}
