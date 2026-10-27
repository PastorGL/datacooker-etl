/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.stream.Stream;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DataHelper {
    public static Stream<String> takeFromPart(JavaPairRDD<Object, DataRecord<?>> rdd, final int part, final int limit) {
        if ((part < 0) || (part >= rdd.getNumPartitions())) {
            return Stream.empty();
        }

        return RetainerRDD.retain(rdd, new int[]{part})
                .map(t -> {
                    Tuple2<?, ?> tt = (Tuple2<?, ?>) t;
                    return tt._1 + " => " + tt._2;
                })
                .collect()
                .stream();
    }
}
