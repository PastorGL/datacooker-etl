/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GeoJsonToRoadMapTest {
    @Test
    public void polygonOutputsTest() {
        try (TestRunner underTest = new TestRunner("/test.geoJsonToRoadMap.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("geometries");
            assertEquals(
                    44,
                    rddS.count()
            );
        }
    }
}
