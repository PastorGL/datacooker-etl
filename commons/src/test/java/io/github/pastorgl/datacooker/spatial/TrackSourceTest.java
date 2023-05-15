/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TrackSourceTest {
    @Test
    public void gpxSourceOutputTest() {
        try (TestRunner underTest = new TestRunner("/test.gpxToTrack.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("source");
            assertEquals(
                    12,
                    rddS.count()
            );

            rddS = ret.get("out");
            assertEquals(
                    12,
                    rddS.count()
            );
        }
    }
}
