/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;

public interface RDDUtils {

    <T> Broadcast<T> broadcast(T broadcast);

    <T> JavaRDD union(JavaRDD... rddArray);

    <K,V> JavaPairRDD union(JavaPairRDD... rddArray);

    <T> JavaRDD parallelize(List<T> list, int partCount);

    <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> list, int partCount);

    <T> JavaRDD<T> empty();
}
