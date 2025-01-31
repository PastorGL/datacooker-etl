/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class StructuredTest {
    @Test
    public void sourceAndSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.structured.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("select1");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<DataRecord<?>> points = rddS.values().collect();

            for (DataRecord<?> t : points) {
                assertNotNull(t.asString("custom"));
                assertTrue(t.asDouble("lon") > 0.D);
            }
        }
    }

    @Test
    public void complexSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.complex.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("select1");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = ret.get("select2");
            assertEquals(
                    1,
                    rddS.count()
            );
            rddS = ret.get("select3");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = ret.get("select4");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = ret.get("select5");
            assertEquals(
                    1,
                    rddS.count()
            );
        }
    }
}
