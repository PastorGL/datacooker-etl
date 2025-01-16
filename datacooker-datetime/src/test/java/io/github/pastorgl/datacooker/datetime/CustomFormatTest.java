/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

public class CustomFormatTest {
    @Test
    public void customTimestampFormatTest() {
        try (TestRunner underTest = new TestRunner("/test.DT_FORMAT.tdl")) {
            Map<String, JavaPairRDD<Object, DataRecord<?>>> res = underTest.go();

            JavaRDD<DataRecord<?>> source = res.get("signals").values();
            JavaRDD<DataRecord<?>> dataset = res.get("signals_output").values();

            Assert.assertEquals(10, dataset.count());

            List<DataRecord<?>> srcCol = source.collect();
            List<DataRecord<?>> collected = dataset.collect();

            Map<Integer, String> srcParsed = srcCol.stream()
                    .collect(Collectors.toMap(
                            l -> l.asInt("ordinal"),
                            l -> l.asString("timestamp")
                    ));
            Map<Integer, String> collParsed = collected.stream()
                    .collect(Collectors.toMap(
                            l -> l.asInt("ordinal"),
                            l -> l.asString("_output_date")
                    ));

            for (Map.Entry<Integer, String> s : srcParsed.entrySet()) {
                assertFalse(collParsed.get(s.getKey()).equals(s.getValue()));
            }
        }
    }
}
