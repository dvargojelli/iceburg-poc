package com.dv.ice;

public class IcebergException extends Exception{
    public IcebergException(String message){
        super(message);
    }

    public IcebergException(String message, Throwable cause){
        super(message, cause);
    }
}
