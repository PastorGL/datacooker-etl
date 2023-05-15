/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AnalyzeTest {
    @Test
    public void analyzeTest() {
        try (TestRunner underTest = new TestRunner("/test.analyze.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> output = underTest.go();

            Record<?> metrics = output.get("_metrics").values().collect().get(0);

            assertEquals("signals", metrics.asString("_streamName"));
            assertEquals("Columnar", metrics.asString("_streamType"));
            assertEquals("counter", metrics.asString("_counterColumn"));
            assertEquals(3L, metrics.asLong("_totalCount").longValue());
            assertEquals(2L, metrics.asLong("_uniqueCounters").longValue());
            assertEquals(1.5D, metrics.asDouble("_counterAverage"), 0.D);
            assertEquals(1.5D, metrics.asDouble("_counterMedian"), 0.D);
        }
    }
}
