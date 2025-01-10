/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class AnalyzeTest {
    @Test
    public void analyzeTest() {
        try (TestRunner underTest = new TestRunner("/test.analyze.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> output = underTest.go();

            DataRecord<?> metrics = output.get("_metrics").values().collect().get(0);

            assertEquals("signals", metrics.asString("_name"));
            assertEquals("Columnar", metrics.asString("_type"));
            assertEquals("counter", metrics.asString("_counter"));
            assertEquals(1L, metrics.asInt("_parts").longValue());
            assertEquals(3L, metrics.asLong("_total").longValue());
            assertEquals(2L, metrics.asLong("_unique").longValue());
            assertEquals(1.5D, metrics.asDouble("_average"), 0.D);
            assertEquals(1.5D, metrics.asDouble("_median"), 0.D);
        }
    }

    @Test
    public void analyzeDeepTest() {
        try (TestRunner underTest = new TestRunner("/test.analyzeDeep.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> output = underTest.go();

            List<DataRecord<?>> metrics = output.get("_metrics_signals").values().collect();

            assertEquals(5, metrics.size());

            assertEquals("uid", metrics.get(0).asString("_counter"));
            assertEquals(0L, metrics.get(0).asInt("_part").longValue());
            assertTrue(metrics.get(0).asInt("_total") > 0);
            assertSame(metrics.get(0).asInt("_total"), metrics.get(0).asInt("_unique"));
            assertEquals(1.D, metrics.get(0).asDouble("_average"), 0.D);
            assertEquals(1.D, metrics.get(0).asDouble("_median"), 0.D);
        }
    }
}
