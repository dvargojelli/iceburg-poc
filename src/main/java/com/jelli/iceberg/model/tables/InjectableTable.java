package com.jelli.iceberg.model.tables;

import org.apache.iceberg.types.Type;
import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Component
public @interface InjectableTable {
    String nameSpace();
    String tableName();

    InjectableColumn[] columns();

    Class<? extends ColumnFactory> columnFactory() default ColumnFactory.INLINE_FACTORY.class();
    <T extends PartitionFactory> Class<T> partitionFactory() default null;
}
