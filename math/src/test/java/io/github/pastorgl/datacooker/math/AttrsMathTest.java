/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import com.google.common.primitives.Doubles;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class AttrsMathTest {
    @Test
    @SuppressWarnings("unchecked")
    public void attrsMathTest() {
        try (TestRunner underTest = new TestRunner("/test.attrsMath.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> ret = underTest.go();

            List<DataRecord<?>> res = ret.get("min_max").values().collect();
            for (DataRecord<?> t : res) {
                double min = Doubles.min(t.asDouble("score1"), t.asDouble("score2"), t.asDouble("score3"));
                double max = Doubles.max(t.asDouble("score2"), t.asDouble("score3"), t.asDouble("score12"));
                assertEquals(min, t.asDouble("min"), 1.E-6);
                assertEquals(max, t.asDouble("max"), 1.E-6);
            }

            res = ret.get("median").values().collect();
            for (DataRecord<?> t : res) {
                List<Double> dd = Stream.of(t.asDouble("score17"), t.asDouble("score18"), t.asDouble("score19"))
                        .sorted().collect(Collectors.toList());

                assertEquals(dd.get(1), t.asDouble("med"), 1.E-6);
            }
        }
    }
}