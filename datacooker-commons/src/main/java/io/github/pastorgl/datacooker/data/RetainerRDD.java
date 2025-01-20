/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.CoalescedRDD;
import org.apache.spark.rdd.CoalescedRDDPartition;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.TaskLocation;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RetainerRDD<T> extends CoalescedRDD {
    private final RDD prev;
    private final int[] partsToRetain;

    public RetainerRDD(RDD<?> oneParent, int[] partsToRetain, ClassTag ct) {
        super(oneParent, partsToRetain.length, Option.empty(), ct);

        this.prev = oneParent;
        this.partsToRetain = partsToRetain;
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] ret = new Partition[partsToRetain.length];
        for (int i = 0; i < ret.length; i++) {
            Seq<TaskLocation> preferredLocs = prev.context().getPreferredLocs(prev, partsToRetain[i]);
            ret[i] = new CoalescedRDDPartition(i, prev, new int[]{partsToRetain[i]}, Option.apply(preferredLocs.isEmpty() ? null :  preferredLocs.head().host()));
        }
        return ret;
    }

    public static JavaPairRDD<Object, DataRecord<?>> retain(JavaPairRDD<Object, DataRecord<?>> rdd, int[] partsToRetain) {
        if (partsToRetain == null) {
            return rdd;
        }

        return new RetainerRDD<Tuple2>(rdd.rdd(), partsToRetain, ClassManifestFactory$.MODULE$.fromClass(Tuple2.class))
                .toJavaRDD()
                .mapToPair(t -> (Tuple2) t);
    }
}
