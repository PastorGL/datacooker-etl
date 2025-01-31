/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class H3FunctionsTest {
    @Test
    public void h3Test() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.H3.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            List<DataRecord<?>> result = ret.get("with_hash").values().collect();
            assertEquals(
                    28,
                    result.size()
            );

            for (DataRecord<?> l : result) {
                if (!SpatialUtils.H3.latLngToCellAddress(l.asDouble("lat"), l.asDouble("lon"), 5).equals(l.asString("_hash"))) {
                    fail();
                }
            }
        }
    }

    @Test
    public void geoH3Test() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.GEO_H3.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            List<DataRecord<?>> result = ret.get("with_hash").values().collect();
            assertEquals(
                    28,
                    result.size()
            );

            for (DataRecord<?> l : result) {
                if (!SpatialUtils.H3.latLngToCellAddress(l.asDouble("lat"), l.asDouble("lon"), 5).equals(l.asString("_hash"))) {
                    fail();
                }
            }
        }
    }
}
