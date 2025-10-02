/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TestDataContext extends DataContext {
    private final static List<String> tempDirs = new ArrayList<>();

    public static void initialize(JavaSparkContext context) {
        try {
            Method create = DataContext.class.getDeclaredMethod("createDataStreams", String.class, String.class, String.class, boolean.class, Map.class, Map.class, int.class, Partitioning.class);
            Method copy = DataContext.class.getDeclaredMethod("copyDataStream", String.class, DataStream.class, String.class, Map.class, Map.class);

        } catch (Exception ignore) {
        }

        DataContext.initialize(context);
    }

    public static ListOrderedMap<String, StreamInfo> createDataStreams(String adapter, String inputName, String path, boolean wildcard, Map<String, Object> params, Map<ObjLvl, List<String>> reqCols, int partCount, Partitioning partitioning) {
        path = "file:" + TestDataContext.class.getResource("/").getPath() + path;

        return DataContext.createDataStreams(adapter, inputName, path, wildcard, params, reqCols, partCount, partitioning);
    }

    public static void copyDataStream(String adapter, DataStream ds, String path, Map<String, Object> params, Map<ObjLvl, List<String>> filterCols) {
        path = System.getProperty("java.io.tmpdir") + "/" + new Date().getTime() + "." + new Random().nextLong() + "/" + path;
        tempDirs.add(path);

        DataContext.copyDataStream(adapter, ds, path, params, filterCols);
    }

    public static void deleteTempDirs() {
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
