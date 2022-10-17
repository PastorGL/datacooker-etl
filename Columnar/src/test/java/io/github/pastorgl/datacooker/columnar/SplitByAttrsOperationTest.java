/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.columnar;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SplitByAttrsOperationTest {
    @Test
    public void splitByColumnTest() {
        try (TestRunner underTest = new TestRunner("/config/test.splitByColumn.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> splitValues = ((JavaRDD<Columnar>) ret.get("split_values")).collect();
            assertEquals(
                    5,
                    splitValues.size()
            );

            for (Columnar split : splitValues) {
                String splitStr = split.asString("city");

                List<Columnar> list = ((JavaRDD<Columnar>) ret.get("city_" + splitStr + "_suff")).collect();

                for (Columnar line : list) {
                    assertEquals(splitStr, line.asString("city"));
                }
            }
        }
    }
}
