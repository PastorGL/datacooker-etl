/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform.functions;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PassthruConverter implements StreamTransformer {
    private final String verb;
    private final String outputName;

    public PassthruConverter(String verb, String outputName) {
        this.verb = verb;
        this.outputName = outputName;
    }

    @Override
    public DataStream apply(DataStream ds, Map<ObjLvl, List<String>> newColumns, Configuration params) {
        List<String> valueColumns = newColumns.get(ObjLvl.VALUE);

        switch (ds.streamType) {
            case Structured:
            case Columnar: {
                if (valueColumns != null) {
                    final List<String> _newColumns = valueColumns;

                    return new DataStreamBuilder(outputName, newColumns)
                            .filtered(verb, ds)
                            .build(ds.rdd().mapPartitionsToPair(it -> {
                                List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, DataRecord<?>> t = it.next();

                                    DataRecord<?> rec = t._2.getClass().getDeclaredConstructor(List.class).newInstance(_newColumns);
                                    for (String col : _newColumns) {
                                        rec.put(col, t._2.asIs(col));
                                    }

                                    ret.add(new Tuple2<>(t._1, rec));
                                }

                                return ret.iterator();
                            }, true));
                }
                break;
            }
            case Point:
            case Polygon: {
                if (valueColumns == null) {
                    valueColumns = (ds.streamType == StreamType.Point) ? newColumns.get(ObjLvl.POINT) : newColumns.get(ObjLvl.POLYGON);
                }

                if (valueColumns != null) {
                    final List<String> _newColumns = valueColumns;

                    return new DataStreamBuilder(outputName, newColumns)
                            .filtered(verb, ds)
                            .build(ds.rdd().mapPartitionsToPair(it -> {
                                List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, DataRecord<?>> t = it.next();

                                    DataRecord<?> rec = t._2.getClass().getDeclaredConstructor(Geometry.class).newInstance((Geometry) t._2);
                                    for (String col : _newColumns) {
                                        rec.put(col, t._2.asIs(col));
                                    }

                                    ret.add(new Tuple2<>(t._1, rec));
                                }

                                return ret.iterator();
                            }, true));
                }
                break;
            }
            case Track: {
                if (valueColumns == null) {
                    valueColumns = newColumns.get(ObjLvl.TRACK);
                }

                final List<String> segmentColumns = newColumns.get(ObjLvl.SEGMENT);
                final List<String> pointColumns = newColumns.get(ObjLvl.POINT);

                if ((valueColumns != null) || (segmentColumns != null) || (pointColumns != null)) {
                    final List<String> _newColumns = valueColumns;

                    return new DataStreamBuilder(outputName, newColumns)
                            .filtered(verb, ds)
                            .build(ds.rdd().mapPartitionsToPair(it -> {
                                List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, DataRecord<?>> t = it.next();

                                    SegmentedTrack src = (SegmentedTrack) t._2;

                                    Geometry[] s = src.geometries();
                                    SegmentedTrack[] seg = new SegmentedTrack[s.length];
                                    for (int i = 0; i < s.length; i++) {
                                        SegmentedTrack ss = (SegmentedTrack) s[i];

                                        Geometry[] p = ss.geometries();
                                        PointEx[] pts = new PointEx[p.length];
                                        for (int j = 0; j < p.length; j++) {
                                            PointEx pp = (PointEx) p[j];

                                            if (pointColumns != null) {
                                                for (String col : pointColumns) {
                                                    pts[j].put(col, pp.asIs(col));
                                                }
                                            } else {
                                                pts[j].put(pp.asIs());
                                            }
                                        }

                                        seg[i] = new SegmentedTrack(pts);
                                        if (segmentColumns != null) {
                                            for (String col : segmentColumns) {
                                                seg[i].put(col, src.asIs(col));
                                            }
                                        } else {
                                            seg[i].put(src.asIs());
                                        }
                                    }

                                    SegmentedTrack out = new SegmentedTrack();

                                    if (_newColumns != null) {
                                        for (String col : _newColumns) {
                                            out.put(col, src.asIs(col));
                                        }
                                    } else {
                                        out.put(src.asIs());
                                    }

                                    ret.add(new Tuple2<>(t._1, out));
                                }

                                return ret.iterator();
                            }, true));
                }
                break;
            }
        }

        return new DataStreamBuilder(outputName, ds.attributes())
                .passedthru(verb, ds)
                .build(ds.rdd());
    }
}
