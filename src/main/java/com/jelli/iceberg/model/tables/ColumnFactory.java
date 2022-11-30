package com.jelli.iceberg.model.tables;

import java.util.List;

@FunctionalInterface
public interface ColumnFactory {
    class InlineColumnFactory asdf implements ColumnFactory{

    }

    List<IcebergColumn> getColumns();
}
