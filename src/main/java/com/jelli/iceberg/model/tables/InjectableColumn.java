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
public @interface InjectableColumn {
    int id();
    String name();
    Type.TypeID type() default Type.TypeID.STRING;
    boolean isRequired() default true;
}
