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


public class DwellTimeOperationTest {
    @Test
    public void dwellTimeTest() {
        try (TestRunner underTest = new TestRunner("/test.dwellTime.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            Map<Object, DataRecord<?>> resMap = ret.get("result").collectAsMap();

            assertEquals(0.36666666666, resMap.get("cell1").asDouble("_dwelltime"), 1E-06);
            assertEquals(0.3D, resMap.get("cell2").asDouble("_dwelltime"), 1E-06);
            assertEquals(0.75D, resMap.get("cell3").asDouble("_dwelltime"), 1E-06);
        }
    }
}
