/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public class TestDataContext extends DataContext {
    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public void createDataStreams(String adapter, String inputName, String path, Map<String, Object> params, int partCount, Partitioning ignore) {
        path = "file:" + getClass().getResource("/").getPath() + path;

        super.createDataStreams(adapter, inputName, path, params, partCount, Partitioning.HASHCODE);
    }
}
