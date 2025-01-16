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

import static org.junit.Assert.assertNotNull;

public class ControlFlowLetTest {
    @Test
    public void letSubqueryTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/testSubquery.let.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("vars");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-YES");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-AB");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-LIST");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void letVarsTest() {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.let.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            JavaPairRDD<Object, DataRecord<?>> rddS = ret.get("vars");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-one");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-two");
            assertNotNull(
                    rddS
            );

            rddS = ret.get("out-three");
            assertNotNull(
                    rddS
            );
        }
    }
}
