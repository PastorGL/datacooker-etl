/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TransformsTest {
    @Test
    public void transformsTest() {
        try (TestRunner underTest = new TestRunner("/test.transforms.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            List<String> result = ret.get("left").values().map(String::valueOf).collect();
            assertEquals(
                    6,
                    result.size()
            );

            for (String r : result) {
                assertEquals('|', r.charAt(3));
            }

            result = ret.get("custom").values().map(String::valueOf).collect();
            assertEquals(
                    6,
                    result.size()
            );

            for (String r : result) {
                String[] split = r.split("\\|");
                assertEquals(3, split.length);
            }
        }
    }
}
