/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PercentRankIncOperationTest {
    @Test
    public void simpleRdd() {
        try (TestRunner underTest = new TestRunner("/test.percentRankInc.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> dataset = ((JavaRDD<Columnar>) ret.get("result")).collect();

            assertEquals(0.D, dataset.get(0).asDouble("_rank"), 1E-06);
            assertEquals(0.875D, dataset.get(8).asDouble("_rank"), 1E-06);
        }
    }

    @Test
    public void pairRdd() {
        try (TestRunner underTest = new TestRunner("/test2.percentRankInc.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<String, Columnar> dataset = (JavaPairRDD<String, Columnar>) ret.get("result");

            Map<String, ArrayList<Double>> resMap = dataset.combineByKey(t -> {
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
