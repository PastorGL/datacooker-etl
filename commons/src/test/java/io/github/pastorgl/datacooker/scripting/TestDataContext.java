/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TestDataContext extends DataContext {
    private final List<String> tempDirs = new ArrayList<>();

    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public Map<String, StreamInfo> createDataStreams(String adapter, String inputName, String path, Map<String, Object> params, int partCount, Partitioning ignore) {
        path = "file:" + getClass().getResource("/").getPath() + path;

        return super.createDataStreams(adapter, inputName, path, params, partCount, Partitioning.HASHCODE);
    }

    @Override
    public void copyDataStream(String adapter, String outputName, String path, Map<String, Object> params) {
        path = System.getProperty("java.io.tmpdir") + "/" + new Date().getTime() + "." + new Random().nextLong() + "/" + path;
        tempDirs.add(path);

        super.copyDataStream(adapter, outputName, path, params);
    }

    public void deleteTempDirs() {
        for (String tempDir : tempDirs) {
            try {
                Path dirPath = Paths.get(tempDir);
                Files.walk(dirPath)
                        .map(Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);
                Files.delete(dirPath);
            } catch (Exception ignore) {
            }
        }
    }
}
