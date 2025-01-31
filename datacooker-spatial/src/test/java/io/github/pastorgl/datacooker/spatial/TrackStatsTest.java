/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TrackStatsTest {
    @Test
    public void trackStatsTest() {
        try (TestRunner underTest = new TestRunner("/test.trackStats.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            List<SegmentedTrack> tracks = ret.get("stats").map(e -> (SegmentedTrack) e._2).collect();
            Assert.assertEquals(
                    12,
                    tracks.size()
            );

            for (SegmentedTrack data : tracks) {
                Assert.assertTrue(data.asDouble("_duration") > 0.D);
                Assert.assertTrue(data.asDouble("_radius") > 0.D);
                Assert.assertTrue(data.asDouble("_distance") > 0.D);
                Assert.assertTrue(data.asDouble("_points") > 0.D);
            }

            SegmentedTrack data = tracks.get(11);
            Assert.assertEquals(14 * 60 + 22, data.asDouble("_duration"), 2);
            Assert.assertEquals(2_488.D, data.asDouble("_distance"), 2);
            Assert.assertEquals(141, data.asInt("_points").intValue());

            data = tracks.get(10);
            Assert.assertEquals(2_864, data.asDouble("_duration"), 2);
            Assert.assertEquals(2_446 * 1.6, data.asDouble("_distance"), 20);
            Assert.assertEquals(287, data.asInt("_points").intValue());
        }
    }
}
