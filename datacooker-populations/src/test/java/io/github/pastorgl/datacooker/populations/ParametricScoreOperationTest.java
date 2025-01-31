/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ParametricScoreOperationTest {
    @Test
    public void calculatePostcodeTest() {
        try (TestRunner underTest = new TestRunner("/test.parametricScore.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            Map<Object, DataRecord<?>> dataset = ret.get("scores").collectAsMap();

            assertEquals(
                    "code-1",
                    dataset.get("59e7074894d2e").asString("_value_1")
            );
            assertEquals(
                    "10.00049",
                    dataset.get("59e7074894d2e").asString("_score_1")
            );

            assertEquals(
                    "code-5",
                    dataset.get("59e7074894e26").asString("_value_1")
            );
            assertEquals(
                    "12.00039",
                    dataset.get("59e7074894e26").asString("_score_1")
            );

            assertEquals(
                    null,
                    dataset.get("59e7074894e26").asString("_value_100")
            );
            assertEquals(
                    null,
                    dataset.get("59e7074894e26").asString("_score_100")
            );
        }
    }
}
