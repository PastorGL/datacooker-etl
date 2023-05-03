/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;
import java.util.Map;

public class DataStream {
    public final StreamType streamType;
    public final JavaPairRDD<Object, Record<?>> rdd;
    int usages = 0;

    public final Accessor<?> accessor;

    public DataStream(StreamType streamType, JavaPairRDD<Object, Record<?>> rdd, Map<String, List<String>> attributes) {
        accessor = streamType.accessor(attributes);

        this.streamType = streamType;
        this.rdd = rdd;
    }

    public DataStream(JavaPairRDD<Object, Record<?>> rdd) {
        accessor = new PlainTextAccessor();

        streamType = StreamType.PlainText;
        this.rdd = rdd;
    }
}
