/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
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
import static org.junit.Assert.assertFalse;

public class SplitByAttrsOperationTest {
    @Test
    public void splitByColumnTest() {
        try (TestRunner underTest = new TestRunner("/test.splitByAttrs.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            List<Record<?>> splitValues = ret.get("split_values").values().collect();
            assertEquals(
                    5,
                    splitValues.size()
            );

            for (Record<?> split : splitValues) {
                String splitStr = split.asString("city");

                List<Record<?>> list = ret.get("city_" + splitStr + "_suff").values().collect();

                assertFalse(list.isEmpty());

                for (Record<?> line : list) {
                    assertEquals(splitStr, line.asString("city"));
                }
            }
        }
    }
}
