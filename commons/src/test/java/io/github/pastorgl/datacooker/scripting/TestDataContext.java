/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public class TestDataContext extends DataContext {
    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public void createDataStream(String inputName, Map<String, Object> params) {
        if (!params.containsKey("path")) {
            throw new InvalidConfigurationException("CREATE DS \"" + inputName + "\" statement must have @path parameter, but it doesn't");
        }

        String path = getClass().getResource("/").getPath() + params.get("path");

        int parts = params.containsKey("part_count") ? ((Number) params.get("part_count")).intValue() : 1;
        JavaRDDLike inputRdd = sparkContext.textFile(path, Math.max(parts, 1));

        inputRdd.rdd().setName("datacooker:input:" + inputName);
        if (store.containsKey(inputName)) {
            throw new InvalidConfigurationException("Can't CREATE DS \"" + inputName + "\", because it is already defined");
        }

        store.put(inputName, new DataStream(inputRdd));
    }
}
