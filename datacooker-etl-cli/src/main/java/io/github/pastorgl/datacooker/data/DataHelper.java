/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq$;
import scala.reflect.ClassManifestFactory$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.Stream;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DataHelper {
    public static Stream<String> takeFromPart(JavaPairRDD<Object, DataRecord<?>> rdd, final int part, final int limit) {
        if ((part < 0) || (part >= rdd.getNumPartitions())) {
            return Stream.empty();
        }

        Object ret = rdd.context().runJob(
                rdd.rdd(),
                (Function1) new Taker(limit),
                Seq$.MODULE$.newBuilder().$plus$eq(part).result(),
                ClassManifestFactory$.MODULE$.fromClass(ArrayList.class)
        );

        return ((ArrayList[]) ret)[0].stream().map(t -> {
            Tuple2<?, ?> tt = (Tuple2<?, ?>) t;
            return tt._1 + " => " + tt._2;
        });
    }

    private static class Taker implements Serializable, Function1<Iterator, ArrayList> {
        private final int limit;

        public Taker(int limit) {
            this.limit = limit;
        }

        @Override
        public ArrayList apply(Iterator it) {
            ArrayList ret = new ArrayList<>();

            int i = 0;
            while (it.hasNext() && (i < limit)) {
                ret.add(it.next());
                i++;
            }

            return ret;
        }
    }
}
