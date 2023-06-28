/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class PolygonStatsTest {
    @Test
    public void polygonStatsTest() {
        try (TestRunner underTest = new TestRunner("/test.polygonStats.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("stats");
            Assert.assertEquals(14, rddS.count());

            List<PolygonEx> datas = rddS.map(e -> (PolygonEx) e._2)
                    .collect();

            for (PolygonEx data : datas) {
                Assert.assertTrue(data.asDouble("_perimeter") > 0.D);
                Assert.assertTrue(data.asDouble("_area") > 0.D);
            }
        }
    }
}
