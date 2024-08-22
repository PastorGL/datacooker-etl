/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

public class DataStream {
    public final StreamType streamType;
    public final JavaPairRDD<Object, DataRecord<?>> rdd;
    public final Accessor accessor;
    public final List<StreamLineage> lineage;

    public final String name;
    private int usages = 0;

    DataStream(String name, StreamType streamType, JavaPairRDD<Object, DataRecord<?>> rdd, Accessor accessor, List<StreamLineage> lineage) {
        this.name = name;

        this.streamType = streamType;
        this.rdd = rdd;
        this.accessor = accessor;
        this.lineage = lineage;
    }

    public int getUsages() {
        return usages;
    }

    public int surpassUsages() {
        if (usages < DataContext.usageThreshold()) {
            usages = DataContext.usageThreshold();
        }
        if (rdd.getStorageLevel() == StorageLevel.NONE()) {
            rdd.persist(DataContext.storageLevel());
        }

        return usages;
    }

    public int incUsages() {
        if ((++usages == DataContext.usageThreshold()) && (rdd.getStorageLevel() == StorageLevel.NONE())) {
            rdd.persist(DataContext.storageLevel());
        }

        return usages;
    }
}
