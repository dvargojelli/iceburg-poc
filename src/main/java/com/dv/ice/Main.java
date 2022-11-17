package com.dv.ice;

import com.dv.ice.examples.IcebergJavaApiUpsert;
import com.dv.ice.examples.Sandbox;

public class Main {

    public static void main(String[] args) throws Exception {
        Sandbox myExample = new Sandbox();
        //IcebergJavaApiUpsert myExample = new IcebergJavaApiUpsert();
        //IcebergPartitionedTable myExample = new IcebergPartitionedTable();
        myExample.run();
    }
}
