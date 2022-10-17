/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FilterByDateOperationTest {
    @Test
    public void filterByDateTest() {
        try (TestRunner underTest = new TestRunner("/test.filterByDate.tdl")) {
            Map<String, JavaRDDLike> result = underTest.go();

            long tsDataCount = result.get("ts_data").count();

            List<Columnar> filtered = result.get("tod").collect();

            assertTrue(0 < filtered.size());
            assertTrue(tsDataCount > filtered.size());

            List<String> months = Arrays.asList("7", "8");

            for (Columnar t : filtered) {
                assertEquals(2016, t.asInt("year").intValue());
                assertTrue(months.contains(t.asString("month")));
            }
        }
    }
}
