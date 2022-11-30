/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataContext;
import org.apache.spark.api.java.JavaSparkContext;

public class TestDataContext extends DataContext {
    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public String inputPathLocal(String name, String path) {
        return getClass().getResource("/").getPath() + path;
    }
}
