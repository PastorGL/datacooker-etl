/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowComplexTest {
    public static Map PROPS = new HashMap() {{
        put("YES", true);
        put("LIST", new Integer[]{1, 2, 3});
        put("AB", new String[]{"a", "b"});
    }};

    @Test
    public void simpleBranchingTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testSimpleBranching.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("expected2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = ret.get("unexpected2");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void nestedBranchingTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNestedBranching.if.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
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

            rddS = ret.get("unexpected2");
            assertNull(
                    rddS
            );

            rddS = ret.get("unexpected3");
            assertNull(
                    rddS
            );

            rddS = ret.get("unexpected4");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void branchingNestedLoopTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNestedBranching.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-3");
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
    public void loopNestedLoopTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNested.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-a-1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-a-2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-a-3");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-b-1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-b-2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-b-3");
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
    public void nightmareModeTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNightmareMode.loop.tdl", PROPS)) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> rddS = ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("yup");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-1");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-2");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-3");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-a");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("signals-b");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("nope");
            assertNull(
                    rddS
            );

            rddS = ret.get("none");
            assertNull(
                    rddS
            );

            rddS = ret.get("nay");
            assertNull(
                    rddS
            );

            rddS = ret.get("yep");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("yap");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("yop");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("yass");
            assertNotNull(
                    rddS
            );
        }
    }
}
