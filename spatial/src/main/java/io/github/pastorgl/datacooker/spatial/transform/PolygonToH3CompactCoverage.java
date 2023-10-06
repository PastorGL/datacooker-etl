/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PolygonToH3CompactCoverage extends Transform {
    static final String HASH_LEVEL_TO = "hash_level_to";
    static final String HASH_LEVEL_FROM = "hash_level_from";
    static final String GEN_HASH = "_hash";
    static final String GEN_LEVEL = "_level";
    static final String GEN_PARENT = "_parent";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3CompactCoverage", StreamType.Polygon, StreamType.Columnar,
                "Take a Polygon DataStream (with Polygons sized as of a country) and generates" +
                        " a Columnar one with compact H3 coverage for each Polygon." +
                        " Does not preserve partitioning",

                new DefinitionMetaBuilder()
                        .def(HASH_LEVEL_TO, "Level of the hash of the finest coverage unit",
                                Integer.class, 9, "Default finest hash level")
                        .def(HASH_LEVEL_FROM, "Level of the hash of the coarsest coverage unit",
                                Integer.class, 1, "Default coarsest hash level")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_HASH, "Polygon H3 hash")
                        .genCol(GEN_LEVEL, "H3 hash level")
                        .genCol(GEN_PARENT, "Parent Polygon H3 hash")
                        .build(),
                true
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_POLYGON);
            }

            final List<String> _outputColumns = valueColumns;

            final Integer levelTo = params.get(HASH_LEVEL_TO);
            final Integer levelFrom = params.get(HASH_LEVEL_FROM);

            JavaPairRDD<Long, Record<?>> hashedGeometries = ds.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Long, Record<?>>> ret = new ArrayList<>();
                        Random random = new Random();

                        while (it.hasNext()) {
                            ret.add(new Tuple2<>(random.nextLong(), it.next()._2));
                        }

                        return ret.iterator();
                    });

            final GeometryFactory geometryFactory = new GeometryFactory();

            final int partCount = hashedGeometries.getNumPartitions();

            for (int lvl = levelFrom; lvl <= levelTo; lvl++) {
                final int _level = lvl;

                hashedGeometries = hashedGeometries
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Long, Record<?>>> ret = new ArrayList<>();

                            H3Core h3 = H3Core.newInstance();

                            while (it.hasNext()) {
                                Tuple2<Long, Record<?>> o = it.next();

                                PolygonEx p = (PolygonEx) o._2;
                                Map<String, Object> properties = p.asIs();
                                Long parent = o._1;

                                if (!properties.containsKey(GEN_HASH)) {
                                    List<LatLng> gco = new ArrayList<>();
                                    LinearRing shell = p.getExteriorRing();
                                    for (Coordinate c : shell.getCoordinates()) {
                                        gco.add(new LatLng(c.y, c.x));
                                    }

                                    List<LinearRing> holes = new ArrayList<>();

                                    List<List<LatLng>> gci = new ArrayList<>();
                                    for (int i = p.getNumInteriorRing(); i > 0; ) {
                                        List<LatLng> gcii = new ArrayList<>();
                                        LinearRing hole = p.getInteriorRingN(--i);

                                        if (_level != levelTo) {
                                            holes.add(hole);
                                        }

                                        for (Coordinate c : hole.getCoordinates()) {
                                            gcii.add(new LatLng(c.y, c.x));
                                        }
                                        gci.add(gcii);
                                    }

                                    Set<Long> polyfill = new HashSet<>(h3.polygonToCells(gco, gci, _level));
                                    Set<Long> hashes = new HashSet<>();
                                    for (long hash : polyfill) {
                                        List<LatLng> geo = h3.cellToBoundary(hash);
                                        geo.add(geo.get(0));

                                        List<Coordinate> cl = new ArrayList<>();
                                        geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                                        PolygonEx polygon = new PolygonEx(geometryFactory.createPolygon(cl.toArray(new Coordinate[0])));
                                        polygon.put(GEN_HASH, Long.toHexString(hash));
                                        polygon.put(GEN_LEVEL, _level);
                                        polygon.put(GEN_PARENT, parent);

                                        if (_level == levelTo) {
                                            List<Long> neighood = h3.gridDisk(hash, 1);
                                            neighood.forEach(neighash -> {
                                                if (!hashes.contains(neighash)) {
                                                    List<LatLng> ng = h3.cellToBoundary(neighash);
                                                    ng.add(ng.get(0));

                                                    List<Coordinate> cn = new ArrayList<>();
                                                    ng.forEach(c -> cn.add(new Coordinate(c.lng, c.lat)));

                                                    PolygonEx neighpoly = new PolygonEx(geometryFactory.createPolygon(cn.toArray(new Coordinate[0])));
                                                    neighpoly.put(properties);
                                                    neighpoly.put(GEN_HASH, Long.toHexString(neighash));
                                                    neighpoly.put(GEN_LEVEL, _level);
                                                    neighpoly.put(GEN_PARENT, parent);

                                                    ret.add(new Tuple2<>(o._1, neighpoly));
                                                    hashes.add(neighash);
                                                }
                                            });

                                            if (!hashes.contains(hash)) {
                                                ret.add(new Tuple2<>(o._1, polygon));
                                                hashes.add(hash);
                                            }
                                        } else {
                                            if (polyfill.containsAll(h3.gridDisk(hash, 1))) {
                                                Collections.reverse(cl);
                                                LinearRing hole = geometryFactory.createLinearRing(cl.toArray(new Coordinate[0]));
                                                holes.add(hole);

                                                ret.add(new Tuple2<>(o._1, polygon));
                                            }
                                        }
                                    }

                                    if (_level != levelTo) {
                                        PolygonEx nextPoly = new PolygonEx(geometryFactory.createPolygon(shell, holes.toArray(new LinearRing[0])));
                                        nextPoly.put(properties);

                                        ret.add(new Tuple2<>(o._1, nextPoly));
                                    }
                                } else {
                                    ret.add(o);
                                }
                            }

                            return ret.iterator();
                        });
            }

            return new DataStream(StreamType.Columnar, hashedGeometries
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Long, Record<?>> p = it.next();

                            Map<String, Object> props = p._2.asIs();

                            Columnar rec = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                rec.put(col, props.get(col));
                            }

                            ret.add(new Tuple2<>(p._1, rec));
                        }

                        return ret.iterator();
                    }), Collections.singletonMap(OBJLVL_VALUE, _outputColumns));
        };
    }
}
