/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ProximityOperationTest {
    @Test
    public void proximityFilterTest() {
        try (TestRunner underTest = new TestRunner("/test.proximity.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = ret.get("target").map(e -> (PointEx) e._2);

            assertEquals(44, resultRDD.count());

            List<PointEx> sample = resultRDD.collect();
            for (PointEx s : sample) {
                assertTrue(s.asDouble("_distance") <= 30000.D);
            }

            JavaRDD<PointEx> evicted = ret.get("evicted").map(e -> (PointEx) e._2);

            assertTrue(2761 - 44 >= evicted.count());

            List<PointEx> evs = evicted.sample(false, 0.01).collect();
            List<PointEx> prox = ret.get("geometries").map(e -> (PointEx) e._2).collect();
            for (PointEx e : evs) {
                for (PointEx x : prox) {
                    double dist = Geodesic.WGS84.Inverse(e.getY(), e.getX(), x.getY(), x.getX(), GeodesicMask.DISTANCE).s12;
                    assertTrue(dist > 30000.D);
                }
            }
        }
    }
}
