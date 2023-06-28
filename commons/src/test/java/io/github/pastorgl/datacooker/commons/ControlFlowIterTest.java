/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static io.github.pastorgl.datacooker.commons.ControlFlowComplexTest.PROPS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowIterTest {
    @Test
    public void iterArrayTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testArray.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals3");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterElseSetTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testElse.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterElseUnsetTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testElseSet.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterNoDefaultsTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNo.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("unexpected");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void iterSetTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals3");
            assertNotNull(
                    rddS
            );
        }
    }
}
