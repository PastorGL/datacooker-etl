/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
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
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected2");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void nestedBranchingTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNestedBranching.if.tdl", PROPS)) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected2");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected3");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected4");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void branchingNestedLoopTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNestedBranching.loop.tdl", PROPS)) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void loopNestedLoopTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNested.loop.tdl", PROPS)) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );
        }
    }


    @Test
    public void nightmareModeTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testNightmareMode.loop.tdl", PROPS)) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yup");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("nope");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("none");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("nay");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yep");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yap");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yop");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yass");
            assertNotNull(
                    rddS
            );
        }
    }
}
