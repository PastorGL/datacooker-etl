/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReachOperationTest {
    @Test
    public void reachTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.reach.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            Map<String, Columnar> resMap = ((JavaPairRDD<String, Columnar>) ret.get("result")).collectAsMap();

            assertEquals(1.0D, resMap.get("gid-all").asDouble("_reach"), 1.E-9D);
            assertEquals(0.1D, resMap.get("gid-onlyone").asDouble("_reach"), 1.E-9D);
            assertEquals(0.6D, resMap.get("gid-some").asDouble("_reach"), 1.E-9D);
        }
    }
}
