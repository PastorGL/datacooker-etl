/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TimezoneConversionTest {
    @Test
    public void timezoneOperationTest() {
        try (TestRunner underTest = new TestRunner("/test.timezone.tdl")) {
            JavaRDD<Record<?>> dataset = underTest.go().get("signals_output").values();

            Assert.assertEquals(5000, dataset.count());

            Record<?> sample = dataset.first();

            Assert.assertEquals(0, sample.asInt("id").intValue());
            Assert.assertEquals(51.09022, sample.asDouble("lat"), 0.D);
            Assert.assertEquals(1.543081, sample.asDouble("lon"), 0.D);
            Assert.assertEquals("59a3e4ffd1a19", sample.asString("userid"));
            Assert.assertEquals(1469583507L, sample.asLong("timestamp").longValue());
            Assert.assertEquals("2016-07-27T05:38:27+04:00[Europe/Samara]", sample.asString("_input_date"));
            Assert.assertEquals(3, sample.asInt("_input_dow_int").intValue());
            Assert.assertEquals(27, sample.asInt("_input_day_int").intValue());
            Assert.assertEquals(7, sample.asInt("_input_month_int").intValue());
            Assert.assertEquals(2016, sample.asInt("_input_year_int").intValue());
            Assert.assertEquals(5, sample.asInt("_input_hour_int").intValue());
            Assert.assertEquals(38, sample.asInt("_input_minute_int").intValue());
            Assert.assertEquals("2016-07-27T01:38:27Z[GMT]", sample.asString("_output_date"));
            Assert.assertEquals(3, sample.asInt("_output_dow_int").intValue());
            Assert.assertEquals(27, sample.asInt("_output_day_int").intValue());
            Assert.assertEquals(7, sample.asInt("_output_month_int").intValue());
            Assert.assertEquals(2016, sample.asInt("_output_year_int").intValue());
            Assert.assertEquals(1, sample.asInt("_output_hour_int").intValue());
            Assert.assertEquals(38, sample.asInt("_output_minute_int").intValue());
        }
    }
}
