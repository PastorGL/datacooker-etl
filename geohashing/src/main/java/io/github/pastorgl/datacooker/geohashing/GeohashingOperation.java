/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
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
    public static final String LAT_COLUMN = "lat_column";
    public static final String LON_COLUMN = "lon_column";
    static final String DEF_CENTER_LAT = "_center_lat";
    static final String DEF_CENTER_LON = "_center_lon";

    public static final String HASH_LEVEL = "hash_level";
    public static final String GEN_HASH = "_hash";

    protected Integer level;
    private String latColumn;
    private String lonColumn;

    @Override
    public void configure() throws InvalidConfigurationException {
        latColumn = params.get(LAT_COLUMN);
        lonColumn = params.get(LON_COLUMN);

        level = params.get(HASH_LEVEL);

        if (level < getMinLevel() || level > getMaxLevel()) {
            throw new InvalidConfigurationException("Geohash level must fall into interval '" + getMinLevel() + "'..'" + getMaxLevel() + "' but is '" + level + "' in the Operation '" + meta.verb + "'");
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        DataStream inputCoords = inputStreams.getValue(0);
        List<String> outColumns = new ArrayList<>(inputCoords.accessor.attributes().get(OBJLVL_VALUE));
        outColumns.add("_hash");

        JavaRDD out;
        if (inputCoords.streamType == StreamType.Columnar) {
            JavaRDD<Columnar> inp = (JavaRDD<Columnar>) inputCoords.get();

            final String _latColumn = latColumn;
            final String _lonColumn = lonColumn;
            final HasherFunction<Columnar> _hasher = getHasher();

            out = inp
                    .mapPartitions(it -> {
                        List<Tuple3<Double, Double, Columnar>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar v = it.next();

                            Double lat = v.asDouble(_latColumn);
                            Double lon = v.asDouble(_lonColumn);

                            ret.add(new Tuple3<>(lat, lon, v));
                        }

                        return ret.iterator();
                    })
                    .mapPartitions(_hasher)
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, Columnar> v = it.next();

                            String hash = v._1;
                            Columnar r = new Columnar(outColumns);
                            r.put(v._2.asIs());
                            r.put("_hash", hash);

                            ret.add(r);
                        }

                        return ret.iterator();
                    });
        } else {
            JavaRDD<PointEx> inp = (JavaRDD<PointEx>) inputCoords.get();

            final HasherFunction<PointEx> _hasher = getHasher();

            out = inp
                    .mapPartitions(it -> {
                        List<Tuple3<Double, Double, PointEx>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx v = it.next();

                            Double lat = v.getY();
                            Double lon = v.getX();

                            ret.add(new Tuple3<>(lat, lon, v));
                        }

                        return ret.iterator();
                    })
                    .mapPartitions(_hasher)
                    .mapPartitions(it -> {
                        List<PointEx> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, PointEx> v = it.next();

                            String hash = v._1;
                            PointEx r = new PointEx(v._2);
                            r.put("_hash", hash);

                            ret.add(r);
                        }

                        return ret.iterator();
                    });
        }

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(inputCoords.streamType, out, new SingletonMap<>(OBJLVL_VALUE, outColumns)));
    }

    protected abstract int getMinLevel();

    protected abstract int getMaxLevel();

    protected abstract Integer getDefaultLevel();

    protected abstract HasherFunction getHasher();
}
