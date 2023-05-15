/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PointJSONSourceTest {
    @Test
    public void sourceTest() {
        try (TestRunner underTest = new TestRunner("/test.geoJsonToPoint.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("source");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<PointEx> points = rddS.map(t -> (PointEx) t._2).collect();

            for (PointEx t : points) {
                Map<String, Object> data = t.asIs();
                assertEquals(2, data.size());
                assertEquals(300.D, t.getRadius(), 0.D);
            }
        }
    }
}
