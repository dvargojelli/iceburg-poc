package com.jelli.iceberg.utils;

import java.util.Objects;

public class Select {

    private static final String ALL_FORMAT = "SELECT %s from %s";

    private String columns;
    private String name;
    private String query;

    public Select(String name){
        this.name = name;
        this.columns = "*";
    }

    public Select columns(String colsToSelect){
        this.columns = colsToSelect;
        return this;
    }

    public String where(String condition){
        if(Objects.isNull(query)){
            query = String.format(ALL_FORMAT, columns, name);
        }
        return query =
                query +
                (query.contains(" where ") ? "" : " where") +
                        String.format(" %s", condition);
    }
    public Select whereAnd(String condition){
        query = where(condition) + " and ";
        return this;
    }
    public String all(){
        return String.format(ALL_FORMAT, name);
    }

}
