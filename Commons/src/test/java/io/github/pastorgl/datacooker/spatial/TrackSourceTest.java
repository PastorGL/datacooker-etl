/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TrackSourceTest {
    @Test
    public void gpxSourceOutputTest() {
        try (TestRunner underTest = new TestRunner("/test.gpxToTrack.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<SegmentedTrack> rddS = (JavaRDD<SegmentedTrack>) ret.get("source");
            assertEquals(
                    12,
                    rddS.count()
            );

            JavaRDD<Object> rddO = (JavaRDD<Object>) ret.get("out");
            assertEquals(
                    12,
                    rddO.count()
            );
        }
    }
}
