/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Structured;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class StructuredTest {
    @Test
    public void sourceAndSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.structured.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Structured> rddS = (JavaRDD<Structured>) ret.get("select1");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<Structured> points = rddS.collect();

            for (Structured t : points) {
                assertNotNull(t.asString("custom"));
                assertTrue(t.asDouble("lon") > 0.D);
            }
        }
    }

    @Test
    public void complexSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.complex.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Structured> rddS = (JavaRDD<Structured>) ret.get("select1");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = (JavaRDD<Structured>) ret.get("select2");
            assertEquals(
                    1,
                    rddS.count()
            );
            rddS = (JavaRDD<Structured>) ret.get("select3");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = (JavaRDD<Structured>) ret.get("select4");
            assertEquals(
                    2,
                    rddS.count()
            );
            rddS = (JavaRDD<Structured>) ret.get("select5");
            assertEquals(
                    1,
                    rddS.count()
            );
        }
    }
}
