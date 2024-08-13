/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PolygonH3SourceTest {
    @Test
    public void polygonH3SourceTest() {
        try (TestRunner underTest = new TestRunner("/test.h3ColumnarToPolygon.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("source");
            assertEquals(
                    10,
                    rddS.count()
            );
        }
    }
}
