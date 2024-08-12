/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SeriesMathOperationTest {
    @Test
    public void seriesTest() {
        try (TestRunner underTest = new TestRunner("/test.seriesMath.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            List<Tuple2<Object, Record<?>>> result = ret.get("normalized").collect();

            assertEquals(
                    6,
                    result.size()
            );

            Map<String, Double> resultMap = result.stream()
                    .collect(Collectors.toMap(t -> t._2.asString("catid") + "," + t._2.asString("userid"), t -> t._2.asDouble("_result")));

            assertEquals(
                    100.D,
                    resultMap.get("280,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    0.D,
                    resultMap.get("237,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );

            result = ret.get("stddev").collect();

            assertEquals(
                    6,
                    result.size()
            );

            resultMap = result.stream()
                    .collect(Collectors.toMap(t -> t._2.asString("catid") + "," + t._2.asString("userid"), t -> t._2.asDouble("_result")));

            assertEquals(
                    0.49925794982803673,
                    resultMap.get("288,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    1.0867241826228398,
                    resultMap.get("280,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    -1.971063876485716,
                    resultMap.get("237,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    -0.3973835870495911,
                    resultMap.get("288,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    0.7233768607484508,
                    resultMap.get("280,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    0.05908847033597929,
                    resultMap.get("237,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
        }
    }
}