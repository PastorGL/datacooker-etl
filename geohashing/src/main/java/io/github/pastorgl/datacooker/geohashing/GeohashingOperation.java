/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.geohashing.functions.HasherFunction;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public abstract class GeohashingOperation extends Operation {
    public static final String LAT_ATTR = "lat_attr";
    public static final String LON_ATTR = "lon_attr";
    static final String DEF_CENTER_LAT = "_center_lat";
    static final String DEF_CENTER_LON = "_center_lon";
    public static final String HASH_LEVEL = "hash_level";

    public static final String GEN_HASH = "_hash";

    protected Integer level;
    private String latAttr;
    private String lonAttr;

    @Override
    public void configure() throws InvalidConfigurationException {
        latAttr = params.get(LAT_ATTR);
        lonAttr = params.get(LON_ATTR);

        level = params.get(HASH_LEVEL);

        if ((level < getMinLevel()) || (level > getMaxLevel())) {
            throw new InvalidConfigurationException("Geohash level must fall into interval '" + getMinLevel() + "'..'" + getMaxLevel() + "' but is '" + level + "' in the Operation '" + meta.verb + "'");
        }
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            List<String> outColumns = new ArrayList<>(input.accessor.attributes().get(OBJLVL_VALUE));
            outColumns.add(GEN_HASH);

            JavaPairRDD<Object, Tuple3<Double, Double, Record<?>>> prep = null;
            switch (input.streamType) {
                case Columnar:
                case Structured: {
                    final String _latColumn = latAttr;
                    final String _lonColumn = lonAttr;

                    prep = input.rdd.mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Tuple3<Double, Double, Record<?>>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> v = it.next();

                            Double lat = v._2.asDouble(_latColumn);
                            Double lon = v._2.asDouble(_lonColumn);

                            ret.add(new Tuple2<>(v._1, new Tuple3<>(lat, lon, v._2)));
                        }

                        return ret.iterator();
                    });
                    break;
                }
                case Point:
                case Track:
                case Polygon: {
                    prep = input.rdd.mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Tuple3<Double, Double, Record<?>>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> v = it.next();

                            Point c = ((SpatialRecord<?>) v._2).getCentroid();
                            Double lat = c.getY();
                            Double lon = c.getX();

                            ret.add(new Tuple2<>(v._1, new Tuple3<>(lat, lon, v._2)));
                        }

                        return ret.iterator();
                    });
                    break;
                }
            }

            JavaPairRDD<Object, Record<?>> out = prep.mapPartitionsToPair(getHasher())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Tuple2<Record<?>, String>> v = it.next();

                            Record<?> r = (Record<?>) v._2._1.clone();
                            r.put(GEN_HASH, v._2._2);

                            ret.add(new Tuple2<>(v._1, r));
                        }

                        return ret.iterator();
                    });

            output.put(outputStreams.get(i), new DataStream(input.streamType, out, new SingletonMap<>(OBJLVL_VALUE, outColumns)));
        }

        return output;
    }

    protected abstract int getMinLevel();

    protected abstract int getMaxLevel();

    protected abstract Integer getDefaultLevel();

    protected abstract HasherFunction getHasher();
}
