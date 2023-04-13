/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public class TestDataContext extends DataContext {
    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public void createDataStream(String inputName, Map<String, Object> params) {
        String path = getClass().getResource("/").getPath() + params.get("path");
        params.put("path", "file:" + path);

        super.createDataStream(inputName, params);
    }
}
