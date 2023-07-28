/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PercentRankIncOperationTest {
    @Test
    public void percentRankAllTest() {
        try (TestRunner underTest = new TestRunner("/test.percentRankInc.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            List<Record<?>> dataset = ret.get("result").values().collect();

            assertEquals(0.D, dataset.get(0).asDouble("_rank"), 1E-06);
            assertEquals(0.875D, dataset.get(8).asDouble("_rank"), 1E-06);
        }
    }

    @Test
    public void perKeyTest() {
        try (TestRunner underTest = new TestRunner("/test2.percentRankInc.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            JavaPairRDD<Object, Record<?>> dataset = ret.get("result");

            Map<Object, ArrayList<Double>> resMap = dataset.combineByKey(t -> {
                        ArrayList<Double> r = new ArrayList<>();
                        r.add(t.asDouble("_rank"));
                        return r;
                    },
                    (l, t) -> {
                        l.add(t.asDouble("_rank"));
                        return l;
                    },
                    (l1, l2) -> {
                        l1.addAll(l2);
                        return l1;
                    }
            ).collectAsMap();

            assertEquals(0.571D, resMap.get("a").get(4), 1E-03);
            assertEquals(0.428D, resMap.get("b").get(4), 1E-03);
            assertEquals(0.428D, resMap.get("c").get(4), 1E-03);
            assertEquals(0.571D, resMap.get("d").get(4), 1E-03);
            assertEquals(0.D, resMap.get("e").get(4), 1E-03);
            assertEquals(0.D, resMap.get("f").get(4), 1E-03);
        }
    }
}
