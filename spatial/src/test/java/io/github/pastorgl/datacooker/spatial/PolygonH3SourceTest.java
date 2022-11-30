/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PolygonH3SourceTest {
    @Test
    public void polygonH3SourceTest() {
        try (TestRunner underTest = new TestRunner("/test.h3ColumnarToPolygon.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PolygonEx> rddS = (JavaRDD<PolygonEx>) ret.get("source");
            assertEquals(
                    10,
                    rddS.count()
            );
        }
    }
}
