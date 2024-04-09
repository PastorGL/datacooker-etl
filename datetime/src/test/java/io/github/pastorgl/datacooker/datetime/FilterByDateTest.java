/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterByDateTest {
    @Test
    public void filterByDateTest() {
        try (TestRunner underTest = new TestRunner("/test.filterByDate.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> result = underTest.go();

            long tsDataCount = result.get("ts_data").count();

            List<Record<?>> filtered = result.get("tod").values().collect();

            assertTrue(0 < filtered.size());
            assertTrue(tsDataCount > filtered.size());

            List<String> months = Arrays.asList("7", "8");

            for (Record<?> t : filtered) {
                assertEquals(2016, t.asInt("year").intValue());
                assertTrue(months.contains(t.asString("month")));
            }
        }
    }
}
