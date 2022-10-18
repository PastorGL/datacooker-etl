/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AreaCoversOperationTest {
    @Test
    public void areaCoversTest() {
        try (TestRunner underTest = new TestRunner("/test.areaCovers.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = (JavaRDD<PointEx>) ret.get("joined");

            Assert.assertEquals(45, resultRDD.count());
        }
    }

    @Test
    public void areaFilterTest() {
        try (TestRunner underTest = new TestRunner("/test2.areaCovers.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = (JavaRDD<PointEx>) ret.get("filtered");

            assertEquals(718, resultRDD.count());

            resultRDD = (JavaRDD) ret.get("evicted");

            assertEquals(4, resultRDD.count());
        }
    }
}
