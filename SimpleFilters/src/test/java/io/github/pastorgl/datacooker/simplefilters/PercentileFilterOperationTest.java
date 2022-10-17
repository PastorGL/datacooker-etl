/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.simplefilters;

import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PercentileFilterOperationTest {
    @Test
    public void percentileFilterTest() {
        try (TestRunner underTest = new TestRunner("/test.percentileFilter.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD signalsRDD = (JavaRDD) ret.get("signals");
            assertEquals(
                    28,
                    signalsRDD.count()
            );

            JavaRDD resultRDD = (JavaRDD) ret.get("filtered");

            assertEquals(
                    21,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD) ret.get("filtered_top");

            assertEquals(
                    23,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD) ret.get("filtered_bottom");

            assertEquals(
                    26,
                    resultRDD.count()
            );
        }
    }
}
