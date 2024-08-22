/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;


public class FrequencyOperationTest {
    @Test
    public void frequencyTest() {
        try (TestRunner underTest = new TestRunner("/test.frequency.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            Map<Object, DataRecord<?>> resMap = ret.get("result").collectAsMap();

            assertEquals(0.25D, resMap.get("2").asDouble("_frequency"), 1E-06);
            assertEquals(1.D / 3.D, resMap.get("4").asDouble("_frequency"), 1E-06);
        }
    }
}
