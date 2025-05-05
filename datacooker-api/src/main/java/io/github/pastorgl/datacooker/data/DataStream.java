/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.Map;

public abstract class DataStream {
    public final StreamType streamType;
    public final List<StreamLineage> lineage;
    public final String keyExpr;

    JavaPairRDD<Object, DataRecord<?>> rdd;

    public final String name;
    private int usages = 0;

    protected DataStream(String name, StreamType streamType, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, String keyExpr) {
        this.name = name;

        this.streamType = streamType;
        this.rdd = rdd;
        this.lineage = lineage;
        this.keyExpr = keyExpr;
    }

    public int getUsages() {
        return usages;
    }

    public JavaPairRDD<Object, DataRecord<?>> rdd() {
        usages++;

        return rdd;
    }

    public StorageLevel getStorageLevel() {
        return rdd.getStorageLevel();
    }

    public int getNumPartitions() {
        return rdd.getNumPartitions();
    }

    abstract public Map<ObjLvl, List<String>> attributes();

    abstract public List<String> attributes(ObjLvl objLvl);

    abstract public DataRecord<?> itemTemplate();
}
