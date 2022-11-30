/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class AntiJoinTest {
    @Test
    public void antiJoinTest() {
        try (TestRunner underTest = new TestRunner("/test.antiJoin.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<String> subtrahendLines = ((JavaPairRDD<Object, Object>) ret.get("subtrahend")).values().map(String::valueOf).collect();
            List<String> minuendLines = ((JavaPairRDD<Object, Object>) ret.get("minuend")).values().map(String::valueOf).collect();

            assertFalse(subtrahendLines.isEmpty());
            assertFalse(minuendLines.isEmpty());

            List<String> diff = new ArrayList<>(((JavaPairRDD<Object, Object>) ret.get("difference")).values().map(String::valueOf).collect());
            Collections.sort(diff);

            assertFalse(diff.isEmpty());

            List<String> expectedDiff = minuendLines.stream()
                    .filter(s -> !subtrahendLines.contains(s))
                    .collect(Collectors.toList());
            Collections.sort(expectedDiff);

            assertEquals(expectedDiff, diff);
            assertFalse(subtrahendLines.stream().anyMatch(diff::contains));
            assertTrue(minuendLines.containsAll(diff));
        }
    }
}
