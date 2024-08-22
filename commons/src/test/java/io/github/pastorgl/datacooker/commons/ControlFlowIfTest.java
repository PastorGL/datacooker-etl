/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static io.github.pastorgl.datacooker.commons.ControlFlowComplexTest.PROPS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowIfTest {
    @Test
    public void ifDefaultsTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testDefault.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void ifElseSetTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNotElse.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
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
    public void ifElseUnsetTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testElse.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
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
    public void ifNestedTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNested.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void ifNestedElseTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNestedElse.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
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
    public void ifNoDefaultsTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNoDefaults.if.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
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
    public void ifTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }
}
