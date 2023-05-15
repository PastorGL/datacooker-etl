/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.geohashing.functions.JapanMeshFunction;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JapanMeshOperationTest {
    @Test
    public void japanMeshAlgoTest() {
        JapanMeshFunction japanMesh = new JapanMeshFunction(5);

        assertEquals("5737144744", japanMesh.getHash(38.12403, 137.59835));

        japanMesh = new JapanMeshFunction(6);
        assertEquals("49323444444", japanMesh.getHash(32.95776, 132.56197));
        assertEquals("39270594423", japanMesh.getHash(26.08107, 127.68476));

        //border point
        japanMesh = new JapanMeshFunction(3);
        assertEquals("64414277", japanMesh.getHash(43.05833334, 141.33750000));
    }

    @Test
    public void japanMeshTest() {
        try (TestRunner underTest = new TestRunner("/test.japanMesh.tdl")) {
            Map<String, JavaPairRDD<Object, Record<?>>> ret = underTest.go();

            List<Record<?>> result = ret.get("with_hash").values().collect();
            assertEquals(
                    28,
                    result.size()
            );

            JapanMeshFunction japanMesh = new JapanMeshFunction(5);
            for (Record<?> l : result) {
                if (!japanMesh.getHash(l.asDouble("lat"), l.asDouble("lon")).equals(l.asString("_hash"))) {
                    fail();
                }
            }
        }
    }
}
