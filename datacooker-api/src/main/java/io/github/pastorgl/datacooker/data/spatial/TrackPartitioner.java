/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import org.apache.spark.HashPartitioner;
import org.apache.spark.util.Utils;
import scala.Tuple2;

public class TrackPartitioner extends HashPartitioner {
    public TrackPartitioner(int partitions) {
        super(partitions);
    }

    @Override
    public int getPartition(Object key) {
        if (key == null) {
            return 0;
        }

        Tuple2<?, ?> k = (Tuple2<?, ?>) key;
        return Utils.nonNegativeMod(k._1.hashCode(), numPartitions());
    }
}
