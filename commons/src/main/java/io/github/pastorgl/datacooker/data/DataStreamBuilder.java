/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DataStreamBuilder {
    private final String name;
    private final Map<String, List<String>> attributes;
    private final StreamType streamType;
    private final List<StreamLineage> lineage = new ArrayList<>();

    public DataStreamBuilder(String name, StreamType streamType, Map<String, List<String>> attributes) {
        this.name = name;

        this.streamType = streamType;
        this.attributes = attributes;
    }

    public DataStreamBuilder filtered(String filter, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, filter, StreamOrigin.FILTERED, Collections.singletonList(ancestor.name)));
        return this;
    }

    public DataStreamBuilder generated(String generator, DataStream... ancestors) {
        List<String> names = new ArrayList<>();
        for (DataStream ancestor : ancestors) {
            if (ancestor != null) {
                lineage.addAll(ancestor.lineage);
                names.add(ancestor.name);
            }
        }
        lineage.add(new StreamLineage(name, generator, StreamOrigin.GENERATED, names));
        return this;
    }

    public DataStreamBuilder created(String source, String path) {
        lineage.add(new StreamLineage(name, source, StreamOrigin.CREATED, Collections.singletonList(path)));
        return this;
    }

    public DataStreamBuilder altered(String source, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, source, StreamOrigin.ALTERED));
        return this;
    }

    public DataStreamBuilder passedthru(String source, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, source, StreamOrigin.PASSEDTHRU));
        return this;
    }

    public DataStreamBuilder augmented(String augmenter, DataStream... ancestors) {
        List<String> names = new ArrayList<>();
        for (DataStream ancestor : ancestors) {
            if (ancestor != null) {
                lineage.addAll(ancestor.lineage);
                names.add(ancestor.name);
            }
        }
        lineage.add(new StreamLineage(name, augmenter, StreamOrigin.AUGMENTED, names));
        return this;
    }

    public DataStreamBuilder transformed(String transformer, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, transformer, StreamOrigin.TRANSFORMED));
        return this;
    }

    public DataStream build(JavaPairRDD<Object, Record<?>> rdd) {
        return new DataStream(name, streamType, rdd, streamType.accessor(attributes), lineage);
    }
}
