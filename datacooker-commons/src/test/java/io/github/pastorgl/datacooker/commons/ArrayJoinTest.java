/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ArrayJoinTest {
    @Test
    public void arrayJoinTest() {
        try (TestRunner underTest = new TestRunner("/test.arrayJoin.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("result");

            // [one,a] 9 * 9 + [two,b] 7 * 7
            assertEquals(81 + 49, rddS.count());
        }
    }
}
