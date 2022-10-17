/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitioningTest {
    @Test
    public void partitionTest() {
        try (TestRunner underTest = new TestRunner("/test.partition.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD rddS = (JavaRDD) ret.get("signals");
            assertEquals(
                    8,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD) ret.get("signals1");
            assertEquals(
                    1,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD) ret.get("signals10");
            assertEquals(
                    10,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD) ret.get("signals8_2");
            assertEquals(
                    2,
                    rddS.getNumPartitions()
            );
        }
    }
}
