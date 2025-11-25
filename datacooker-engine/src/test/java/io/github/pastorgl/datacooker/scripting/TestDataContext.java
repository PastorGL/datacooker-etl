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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TestDataContext extends DataContext {
    private final static List<String> tempDirs = new ArrayList<>();

    public ListOrderedMap<String, StreamInfo> createDataStreams(String adapter, String inputName, String path, boolean wildcard, Map<String, Object> params, Map<ObjLvl, List<String>> reqCols, int partCount, Partitioning partitioning) {
        path = "file:" + TestDataContext.class.getResource("/").getPath() + path;

        return super.createDataStreams(adapter, inputName, path, wildcard, params, reqCols, partCount, partitioning);
    }

    public void copyDataStream(String adapter, DataStream ds, String path, Map<String, Object> params, Map<ObjLvl, List<String>> filterCols) {
        path = System.getProperty("java.io.tmpdir") + "/" + new Date().getTime() + "." + new Random().nextLong() + "/" + path;
        tempDirs.add(path);

        super.copyDataStream(adapter, ds, path, params, filterCols);
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
