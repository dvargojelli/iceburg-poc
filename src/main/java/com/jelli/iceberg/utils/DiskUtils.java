package com.jelli.iceberg.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class DiskUtils {

    private static final String TEMP_FOLDER = System.getProperty("java.io.tmpdir");
    public static File writeToDisk(String tableName, InputStream inputStream) throws IOException {
        File targetFile = new File(TEMP_FOLDER + File.separator + tableName + File.separator + System.nanoTime() + ".csv");
        FileUtils.copyInputStreamToFile(inputStream, targetFile);
        return targetFile;
    }
}
