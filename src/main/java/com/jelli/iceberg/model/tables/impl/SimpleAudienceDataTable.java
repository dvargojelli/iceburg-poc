package com.jelli.iceberg.model.tables.impl;

import com.jelli.iceberg.model.tables.IcebergColumn;
import com.jelli.iceberg.model.tables.IcebergIceTable;
import com.jelli.iceberg.utils.Select;
import lombok.Getter;
import lombok.Setter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

@Getter
@Setter
public class SimpleAudienceDataTable extends IcebergIceTable {
    public static Select select(){
        return new Select(SimpleAudienceDataTableConstants.TABLE_ID);
    }

    public SimpleAudienceDataTable() {
        super();
        setSchema(createSchema());
        setPartitionSpec(getPartitionSpec());
        setTableIdentifier(createTableIdentifier());
    }


    public List<IcebergColumn> getColumns() {
        return List.of(
                new IcebergColumn(1, SimpleAudienceDataTableConstants.COLUMN_FCCID, Types.LongType.get(), true),
                new IcebergColumn(2, SimpleAudienceDataTableConstants.COLUMN_CALL_LETTER, Types.StringType.get(), true),
                new IcebergColumn(3, SimpleAudienceDataTableConstants.COLUMN_MARKET_CODE, Types.LongType.get(), true),
                new IcebergColumn(4, SimpleAudienceDataTableConstants.COLUMN_BOOK_NAME, Types.StringType.get(), true),
                new IcebergColumn(5, SimpleAudienceDataTableConstants.COLUMN_EXTERNAL_BOOK_ID, Types.LongType.get(), true),
                new IcebergColumn(6, SimpleAudienceDataTableConstants.COLUMN_AUDIENCE_SEGMENT, Types.StringType.get(), true),
                new IcebergColumn(7, SimpleAudienceDataTableConstants.COLUMN_AUDIENCE_SEGMENT_ID, Types.LongType.get(), true),
                new IcebergColumn(8, SimpleAudienceDataTableConstants.COLUMN_DAYPART, Types.StringType.get(), true),
                new IcebergColumn(9, SimpleAudienceDataTableConstants.COLUMN_DEMO, Types.StringType.get(), true),
                new IcebergColumn(10, SimpleAudienceDataTableConstants.COLUMN_CUME, Types.LongType.get(), true),
                new IcebergColumn(12, SimpleAudienceDataTableConstants.COLUMN_AQH, Types.LongType.get(), true)
        );
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
        return TableIdentifier.parse(SimpleAudienceDataTableConstants.TABLE_ID);
    }


    class SimpleAudienceDataTableConstants{
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
    }

}
