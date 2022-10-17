/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DistinctTest {
    @Test
    public void distinctTest() {
        try (TestRunner underTest = new TestRunner("/test.distinct.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> resultRDD = (JavaRDD<Columnar>) ret.get("distinct");

            assertEquals(
                    60,
                    resultRDD.count()
            );
        }
    }
}
