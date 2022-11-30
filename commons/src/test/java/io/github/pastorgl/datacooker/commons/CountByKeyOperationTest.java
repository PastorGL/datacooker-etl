/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CountByKeyOperationTest {
    @Test
    public void mapCountTest() {
        try (TestRunner underTest = new TestRunner("/test.countByKey.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<Text, Text> left = (JavaPairRDD<Text, Text>) ret.get("left");
            assertEquals(
                    6,
                    left.count()
            );

            Map<Text, Columnar> result = ((JavaPairRDD<Text, Columnar>) ret.get("counted")).collectAsMap();

            assertEquals(
                    3,
                    result.size()
            );

            for (Columnar l : result.values()) {
                assertEquals(2L, l.asLong("_count").longValue());
            }
        }
    }
}
