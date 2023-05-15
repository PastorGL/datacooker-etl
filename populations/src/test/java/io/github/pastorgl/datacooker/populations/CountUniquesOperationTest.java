/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CountUniquesOperationTest {
    @Test
    public void countUniquesTest() {
        try (TestRunner underTest = new TestRunner("/test.countUniques.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            Map<Object, Record<?>> dataset = ret.get("result").collectAsMap();

            assertEquals(10L, dataset.get("gid-all").asLong("userid").longValue());
            assertEquals(1L, dataset.get("gid-onlyone").asLong("userid").longValue());
            assertEquals(6L, dataset.get("gid-some").asLong("userid").longValue());
        }
    }
}
