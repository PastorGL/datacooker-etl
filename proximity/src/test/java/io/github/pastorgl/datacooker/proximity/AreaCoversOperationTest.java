/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AreaCoversOperationTest {
    @Test
    public void areaCoversTest() {
        try (TestRunner underTest = new TestRunner("/test.areaCovers.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> resultRDD = ret.get("joined");

            Assert.assertEquals(45, resultRDD.count());
        }
    }

    @Test
    public void areaFilterTest() {
        try (TestRunner underTest = new TestRunner("/test2.areaCovers.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> resultRDD = ret.get("filtered");

            assertEquals(718, resultRDD.count());

            resultRDD = ret.get("evicted");

            assertEquals(4, resultRDD.count());
        }
    }
}
