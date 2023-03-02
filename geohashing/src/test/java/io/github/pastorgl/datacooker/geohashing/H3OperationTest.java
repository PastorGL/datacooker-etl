/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import com.uber.h3core.H3Core;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class H3OperationTest {
    @Test
    public void h3Test() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.h3.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> result = ((JavaRDD<Columnar>) ret.get("with_hash")).collect();
            assertEquals(
                    28,
                    result.size()
            );

            H3Core h3 = H3Core.newInstance();
            for (Columnar l : result) {
                if (!h3.latLngToCellAddress(l.asDouble("lat"), l.asDouble("lon"), 5).equals(l.asString("_hash"))) {
                    fail();
                }
            }
        }
    }
}
