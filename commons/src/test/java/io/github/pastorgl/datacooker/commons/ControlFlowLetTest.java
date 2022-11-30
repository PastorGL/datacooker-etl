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

import java.util.Map;

import static io.github.pastorgl.datacooker.commons.ControlFlowComplexTest.PROPS;
import static org.junit.Assert.assertNotNull;

public class ControlFlowLetTest {
    @Test
    public void letSubqueryTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testSubquery.let.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("vars");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-YES");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-AB");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-LIST");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void letVarsTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.let.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("vars");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-one");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-two");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-three");
            assertNotNull(
                    rddS
            );
        }
    }
}
