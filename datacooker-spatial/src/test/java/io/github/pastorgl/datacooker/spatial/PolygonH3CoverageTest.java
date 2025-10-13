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

public class PolygonH3CoverageTest {
    @Test
    public void h3CompactCoverageTest() {
        try (TestRunner underTest = new TestRunner("/test.h3CompactCoverage.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("source");
            assertEquals(
                    110,
                    rddS.count()
            );
        }
    }

    @Test
    public void h3UniformCoverageTest() {
        try (TestRunner underTest = new TestRunner("/test.h3UniformCoverage.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("source");
            assertEquals(
                    11,
                    rddS.count()
            );
        }
    }
}
