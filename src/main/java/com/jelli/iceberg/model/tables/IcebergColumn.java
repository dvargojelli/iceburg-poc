package com.jelli.iceberg.model.tables;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.util.TypeUtils;

import java.util.function.Consumer;

@Getter
@AllArgsConstructor
public class IcebergColumn {
    private final Integer id;
    private final String name;
    private final Type.PrimitiveType type;
    private final boolean isRequired;


    public Types.NestedField toIcebergSchema(){
        if(isRequired){
            return Types.NestedField.required(id,name,type);
        }
        return Types.NestedField.optional(id, name, type);
    }
}
