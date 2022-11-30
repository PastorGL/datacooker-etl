/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class KeyedMathOperationTest {
    @Test
    @SuppressWarnings("unchecked")
    public void keyedMathTest() {
        try (TestRunner underTest = new TestRunner("/test.keyedMath.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            String cd27220b = "cd27220b-11e9-4d00-b914-eb567d4df6e7";
            String c7e5a6f9 = "c7e5a6f9-ca03-4554-a046-541ff46cd88b";

            Map<String, Columnar> res = ((JavaPairRDD) ret.get("math")).collectAsMap();

            assertEquals(0.D - 15, res.get(cd27220b).asDouble("sum"), 0.D);
            assertNotEquals(0.D - 15, res.get(c7e5a6f9).asDouble("sum"), 0.D);

            assertEquals(0.D, res.get(cd27220b).asDouble("mean"), 0.D);
            assertNotEquals(0.D, res.get(c7e5a6f9).asDouble("mean"), 0.D);

            assertEquals(222.4748765D, res.get(cd27220b).asDouble("rms"), 1E-7D);
            assertNotEquals(0.D, res.get(c7e5a6f9).asDouble("rms"), 0.D);

            assertEquals(0.D, res.get(cd27220b).asDouble("min"), 0.D);
            assertEquals(0.D, res.get(c7e5a6f9).asDouble("min"), 0.D);

            assertEquals(0.D, res.get(cd27220b).asDouble("max"), 0.D);
            assertEquals(27.5995511D, res.get(c7e5a6f9).asDouble("max"), 1E-6D);

            assertEquals(20196938.49D, res.get(cd27220b).asDouble("mul"), 1E-2D);
            assertNotEquals(0.D, res.get(c7e5a6f9).asDouble("mul"), 0.D);

            res = ((JavaPairRDD) ret.get("median")).collectAsMap();

            assertEquals(280.D, res.get(cd27220b).asDouble("med"), 0.D);
            assertEquals(280.D, res.get(c7e5a6f9).asDouble("med"), 0.D);
        }
    }
}
