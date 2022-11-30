/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ColumnarSelectsTest {
    @Test
    public void columnarSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.columnarSelect.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> rddS = (JavaRDD<Columnar>) ret.get("ret1");

            assertEquals(11, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret2");

            assertEquals(4, rddS.count());
            for (Columnar data : rddS.collect()) {
                double acc = data.asDouble("acc");
                assertTrue(acc >= 15.D);
                assertTrue(acc < 100.D);
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret3");

            assertEquals(15, rddS.count());
            Pattern p = Pattern.compile(".+?non.*");
            for (Columnar data : rddS.collect()) {
                assertTrue("e2e".equals(data.asString("pt")) || p.matcher(data.asString("trackid")).matches());
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret4");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret5");

            assertEquals(37, rddS.count());
            for (Columnar data : rddS.collect()) {
                double acc = data.asDouble("acc");
                assertEquals(-24.02D, acc, 1E-03D);
                long cca = data.asLong("100500");
                assertEquals(100500L, cca);
                assertEquals("immediate", data.asString("'immediate'"));
                assertNull(data.asIs("NULL"));
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret6");

            assertEquals(26, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret7");

            assertEquals(33, rddS.count());
            for (Columnar data : rddS.collect()) {
                double acc = data.asDouble("acc");
                assertTrue(acc < 15.D || acc >= 100.D);
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret8");

            assertEquals(22, rddS.count());
            for (Columnar data : rddS.collect()) {
                assertFalse(!"e2e".equals(data.asString("pt")) && p.matcher(data.asString("trackid")).matches());
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret9");

            assertEquals(0, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret10");

            assertEquals(8, rddS.count());
            for (Columnar data : rddS.collect()) {
                assertTrue(data.asInt("num") >= 8);
                assertTrue(data.asInt("num") <= 15);
            }

            rddS = (JavaRDD<Columnar>) ret.get("ret20");

            assertEquals(3, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret21");

            assertEquals(24, rddS.count());
        }
    }

    @Test
    public void selectByColumnTest() {
        try (TestRunner underTest = new TestRunner("/test2.columnarSelect.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> rddS = (JavaRDD<Columnar>) ret.get("ret11");

            assertEquals(9, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret12");

            assertEquals(28, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret13");

            assertEquals(35, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret14");

            assertEquals(0, rddS.count());
        }
    }

    @Test
    public void selectSubqueryTest() {
        try (TestRunner underTest = new TestRunner("/test.columnarSubquery.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> rddS = (JavaRDD<Columnar>) ret.get("ret1");

            assertEquals(11, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret2");

            assertEquals(30, rddS.count());
        }
    }

    @Test
    public void selectUnionTest() {
        try (TestRunner underTest = new TestRunner("/test.columnarUnion.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> rddS = (JavaRDD<Columnar>) ret.get("union");

            assertEquals(259, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("union_and");

            assertEquals(1, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("union_xor");

            assertEquals(2, rddS.count());
        }
    }

    @Test
    public void selectJoinTest() {
        try (TestRunner underTest = new TestRunner("/test.columnarJoin.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD resultRDD = (JavaPairRDD) ret.get("joined");
            assertEquals(
                    12,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_left");
            assertEquals(
                    6,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_right");
            assertEquals(
                    4,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_outer");
            assertEquals(
                    8,
                    resultRDD.count()
            );
        }
    }

    @Test
    public void selectExpressionsTest() {
        try (TestRunner underTest = new TestRunner("/test3.columnarSelect.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> rddS = (JavaRDD<Columnar>) ret.get("ret1");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret2");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret2");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret3");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret4");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret5");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Columnar>) ret.get("ret6");

            assertEquals(37, rddS.count());
        }
    }
}
