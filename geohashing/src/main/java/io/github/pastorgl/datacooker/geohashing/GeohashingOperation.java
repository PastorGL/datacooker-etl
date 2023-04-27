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
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
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

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        DataStream inputCoords = inputStreams.getValue(0);
        List<String> outColumns = new ArrayList<>(inputCoords.accessor.attributes().get(OBJLVL_VALUE));
        outColumns.add("_hash");

        JavaRDD<Object> inp = (JavaRDD<Object>) inputCoords.get();
        JavaRDD<Tuple3<Double, Double, Record>> prep = null;
        switch (inputCoords.streamType) {
            case Columnar:
            case Structured: {
                final String _latColumn = latAttr;
                final String _lonColumn = lonAttr;

                prep = inp.mapPartitions(it -> {
                    List<Tuple3<Double, Double, Record>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Record v = (Record) it.next();

                        Double lat = v.asDouble(_latColumn);
                        Double lon = v.asDouble(_lonColumn);

                        ret.add(new Tuple3<>(lat, lon, v));
                    }

                    return ret.iterator();
                });
                break;
            }
            case Point:
            case Track:
            case Polygon: {
                prep = inp.mapPartitions(it -> {
                    List<Tuple3<Double, Double, Record>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        SpatialRecord v = (SpatialRecord) it.next();

                        Double lat = v.getCentroid().getY();
                        Double lon = v.getCentroid().getX();

                        ret.add(new Tuple3<>(lat, lon, v));
                    }

                    return ret.iterator();
                });
                break;
            }
        }

        JavaRDD<Object> out = prep.mapPartitions(getHasher())
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, Record> v = it.next();

                        String hash = v._1;
                        Record r = (Record) v._2.clone();
                        r.put(GEN_HASH, hash);

                        ret.add(r);
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(inputCoords.streamType, out, new SingletonMap<>(OBJLVL_VALUE, outColumns)));
    }

    protected abstract int getMinLevel();

    protected abstract int getMaxLevel();

    protected abstract Integer getDefaultLevel();

    protected abstract HasherFunction getHasher();
}
