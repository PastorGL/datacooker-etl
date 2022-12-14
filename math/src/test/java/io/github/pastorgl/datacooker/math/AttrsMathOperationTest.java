/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class AttrsMathOperationTest {
    @Test
    @SuppressWarnings("unchecked")
    public void attrsMathTest() {
        try (TestRunner underTest = new TestRunner("/test.attrsMath.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> res = ((JavaRDD<Columnar>) ret.get("min_max")).collect();
            for (Columnar t : res) {
                double min = Doubles.min(t.asDouble("score1"), t.asDouble("score2"), t.asDouble("score3"));
                double max = Doubles.max(t.asDouble("score2"), t.asDouble("score3"), t.asDouble("score12"));
                assertEquals(min, t.asDouble("min"), 1.E-6);
                assertEquals(max, t.asDouble("max"), 1.E-6);
            }

            res = ((JavaRDD<Columnar>) ret.get("median")).collect();
            for (Columnar t : res) {
                List<Double> dd = Stream.of(t.asDouble("score17"), t.asDouble("score18"), t.asDouble("score19"))
                        .sorted().collect(Collectors.toList());

                assertEquals(dd.get(1), t.asDouble("med"), 1.E-6);
            }
        }
    }
}