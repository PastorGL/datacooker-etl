/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DigestTest {
    @Test
    public void digestTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.digest.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Columnar> resultRDD = (JavaRDD<Columnar>) ret.get("with_digest");

            List<Columnar> list = resultRDD.collect();

            assertEquals(
                    28,
                    list.size()
            );

            MessageDigest md5 = MessageDigest.getInstance("MD5");
            MessageDigest sha1 = MessageDigest.getInstance("SHA1");
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");

            for (Columnar row : list) {
                assertEquals(DatatypeConverter.printHexBinary(md5.digest(row.asBytes("ts"))), row.asString("ts_md5"));
                assertEquals(DatatypeConverter.printHexBinary(sha1.digest(row.asBytes("lat"))), row.asString("lat_sha1"));
                assertEquals(DatatypeConverter.printHexBinary(sha256.digest(row.asBytes("lon"))), row.asString("lon_sha256"));
                md5.update(row.asBytes("lat"));
                md5.update(row.asBytes("lon"));
                assertEquals(DatatypeConverter.printHexBinary(md5.digest()), row.asString("ll_md5"));
            }
        }
    }
}
