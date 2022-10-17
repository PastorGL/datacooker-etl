/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ProximityOperationTest {
    @Test
    public void proximityFilterTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.proximity.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = (JavaRDD<PointEx>) ret.get("target");

            assertEquals(44, resultRDD.count());

            List<PointEx> sample = resultRDD.collect();
            for (PointEx s : sample) {
                assertTrue(s.asDouble("_distance") <= 30000.D);
            }

            JavaRDD<PointEx> evicted = (JavaRDD<PointEx>) ret.get("evicted");

            assertTrue(2761 - 44 >= evicted.count());

            List<PointEx> evs = evicted.sample(false, 0.01).collect();
            List<PointEx> prox = ret.get("geometries").collect();
            for (PointEx e : evs) {
                for (PointEx x : prox) {
                    double dist = Geodesic.WGS84.Inverse(e.getY(), e.getX(), x.getY(), x.getX(), GeodesicMask.DISTANCE).s12;
                    assertTrue(dist > 30000.D);
                }
            }
        }
    }
}
