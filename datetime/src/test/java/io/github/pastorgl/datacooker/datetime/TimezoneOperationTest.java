/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

public class TimezoneOperationTest {
    @Test
    public void timezoneOperationTest() {
        try (TestRunner underTest = new TestRunner("/test2.timezone.tdl")) {
            JavaRDD<Record<?>> dataset = underTest.go().get("signals_output").values();

            Assert.assertEquals(5000, dataset.count());

            Record<?> sample = dataset.first();

            Assert.assertEquals(0, sample.asInt("id").intValue());
            Assert.assertEquals(51.09022, sample.asDouble("lat"), 0.D);
            Assert.assertEquals(1.543081, sample.asDouble("lon"), 0.D);
            Assert.assertEquals("59a3e4ffd1a19", sample.asString("userid"));
            Assert.assertEquals(1469583507L, sample.asLong("timestamp").longValue());
            Assert.assertEquals("2016-07-27T01:38:27+04:00[Europe/Samara]", sample.asString("_input_date"));
            Assert.assertEquals(3, sample.asInt("_input_dow_int").intValue());
            Assert.assertEquals(27, sample.asInt("_input_day_int").intValue());
            Assert.assertEquals(7, sample.asInt("_input_month_int").intValue());
            Assert.assertEquals(2016, sample.asInt("_input_year_int").intValue());
            Assert.assertEquals(1, sample.asInt("_input_hour_int").intValue());
            Assert.assertEquals(38, sample.asInt("_input_minute_int").intValue());
            Assert.assertEquals("2016-07-26T21:38:27Z[GMT]", sample.asString("_output_date"));
            Assert.assertEquals(2, sample.asInt("_output_dow_int").intValue());
            Assert.assertEquals(26, sample.asInt("_output_day_int").intValue());
            Assert.assertEquals(7, sample.asInt("_output_month_int").intValue());
            Assert.assertEquals(2016, sample.asInt("_output_year_int").intValue());
            Assert.assertEquals(21, sample.asInt("_output_hour_int").intValue());
            Assert.assertEquals(38, sample.asInt("_output_minute_int").intValue());
        }
    }

    @Test
    public void customTimestampFormatTest() {
        try (TestRunner underTest = new TestRunner("/test.timezone.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> res = underTest.go();

            JavaRDD<Record<?>> source = res.get("signals").values();
            JavaRDD<Record<?>> dataset = res.get("signals_output").values();

            Assert.assertEquals(10, dataset.count());

            List<Record<?>> srcCol = source.collect();
            List<Record<?>> collected = dataset.collect();

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
                assertFalse(collParsed.get(s.getKey()).equalsIgnoreCase(s.getValue()));
            }
        }
    }
}
