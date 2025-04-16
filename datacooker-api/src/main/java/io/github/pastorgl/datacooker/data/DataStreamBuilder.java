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
    private final Map<ObjLvl, List<String>> attributes;
    private String keyExpr;
    private StreamType streamType;
    private final List<StreamLineage> lineage = new ArrayList<>();

    public DataStreamBuilder(String name, Map<ObjLvl, List<String>> attributes) {
        this.name = name;
        this.attributes = attributes;
    }

    public DataStreamBuilder filtered(String filter, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, filter, StreamOrigin.FILTERED, Collections.singletonList(ancestor.name)));
        streamType = ancestor.streamType;
        keyExpr = ancestor.keyExpr;
        return this;
    }

    public DataStreamBuilder generated(String generator, StreamType asType, DataStream... ancestors) {
        List<String> names = new ArrayList<>();
        if (ancestors != null) {
            for (DataStream ancestor : ancestors) {
                lineage.addAll(ancestor.lineage);
                names.add(ancestor.name);
            }
            if (ancestors.length > 0) {
                keyExpr = ancestors[0].keyExpr;
            }
        }
        lineage.add(new StreamLineage(name, generator, StreamOrigin.GENERATED, names));
        this.streamType = asType;
        return this;
    }

    public DataStreamBuilder created(String source, String path, StreamType asType, String keyExpr) {
        lineage.add(new StreamLineage(name, source, StreamOrigin.CREATED, Collections.singletonList(path)));
        this.streamType = asType;
        this.keyExpr = keyExpr;
        return this;
    }

    public DataStreamBuilder altered(String source, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, source, StreamOrigin.ALTERED));
        this.streamType = ancestor.streamType;
        keyExpr = ancestor.keyExpr;
        return this;
    }

    public DataStreamBuilder passedthru(String source, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, source, StreamOrigin.PASSEDTHRU));
        streamType = ancestor.streamType;
        keyExpr = ancestor.keyExpr;
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
        streamType = ancestors[0].streamType;
        keyExpr = ancestors[0].keyExpr;
        return this;
    }

    public DataStreamBuilder transformed(String transformer, StreamType toType, DataStream ancestor) {
        lineage.addAll(ancestor.lineage);
        lineage.add(new StreamLineage(name, transformer, StreamOrigin.TRANSFORMED));
        this.streamType = toType;
        keyExpr = ancestor.keyExpr;
        return this;
    }

    public DataStreamBuilder keyExpr(String keyExpr) {
        this.keyExpr = keyExpr;
        return this;
    }

    public DataStream build(JavaPairRDD<Object, DataRecord<?>> rdd) {
        return switch (streamType) {
            case PlainText -> new PlainTextDataStream(name, rdd, lineage, keyExpr);
            case Columnar -> new ColumnarDataStream(name, rdd, lineage, attributes, keyExpr);
            case Structured -> new StructuredDataStream(name, rdd, lineage, attributes, keyExpr);
            case Point -> new PointDataStream(name, rdd, lineage, attributes, keyExpr);
            case Track -> new TrackDataStream(name, rdd, lineage, attributes, keyExpr);
            case Polygon -> new PolygonDataStream(name, rdd, lineage, attributes, keyExpr);
            default -> throw new RuntimeException("Unsupported stream type: " + streamType);
        };
    }
}
