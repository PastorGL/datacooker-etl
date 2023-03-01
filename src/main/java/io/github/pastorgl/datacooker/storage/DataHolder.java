package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.data.BinRec;
import org.apache.spark.api.java.JavaRDD;

public class DataHolder {
    public final JavaRDD<BinRec> underlyingRdd;
    public final String sub;

    public DataHolder(JavaRDD<BinRec> underlyingRdd, String sub) {
        this.underlyingRdd = underlyingRdd;
        this.sub = sub;
    }
}
