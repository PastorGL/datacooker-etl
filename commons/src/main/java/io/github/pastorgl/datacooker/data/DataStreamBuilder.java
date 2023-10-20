/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.StreamOrigin;
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

    public DataStreamBuilder filtered(String filter, DataStream... ancestors) {
        List<String> names = new ArrayList<>();
        for (DataStream ancestor : ancestors) {
            if (ancestor != null) {
                lineage.addAll(ancestor.lineage);
                names.add(ancestor.name);
            }
        }
        lineage.add(new StreamLineage(filter, StreamOrigin.FILTERED, names));
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
        lineage.add(new StreamLineage(generator, StreamOrigin.GENERATED, names));
        return this;
    }

    public DataStreamBuilder created(String source, String path) {
        lineage.add(new StreamLineage(source, StreamOrigin.CREATED, Collections.singletonList(path)));
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
        lineage.add(new StreamLineage(augmenter, StreamOrigin.AUGMENTED, names));
        return this;
    }

    public DataStreamBuilder transformed(String transformer, DataStream... ancestors) {
        List<String> names = new ArrayList<>();
        for (DataStream ancestor : ancestors) {
            lineage.addAll(ancestor.lineage);
            names.add(ancestor.name);
        }
        lineage.add(new StreamLineage(transformer, StreamOrigin.TRANSFORMED, names));
        return this;
    }

    public DataStream build(JavaPairRDD<Object, Record<?>> rdd) {
        return new DataStream(name, streamType, rdd, streamType.accessor(attributes), lineage);
    }
}
