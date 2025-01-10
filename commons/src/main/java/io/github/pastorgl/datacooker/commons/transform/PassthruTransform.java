/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unused")
public class PassthruTransform extends Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("passthru", StreamType.Passthru, StreamType.Passthru,
                "Doesn't change a DataStream in any way except optional top-level attribute filtering",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);

            switch (ds.streamType) {
                case Structured:
                case Columnar: {
                    if (valueColumns != null) {
                        final List<String> _newColumns = valueColumns;

                        return new DataStreamBuilder(ds.name, ds.streamType, newColumns)
                                .filtered(meta.verb, ds)
                                .build(ds.rdd.mapPartitionsToPair(it -> {
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
                }
                case Point:
                case Polygon: {
                    if (valueColumns == null) {
                        valueColumns = (ds.streamType == StreamType.Point) ? newColumns.get(OBJLVL_POINT) : newColumns.get(OBJLVL_POLYGON);
                    }

                    if (valueColumns != null) {
                        final List<String> _newColumns = valueColumns;

                        return new DataStreamBuilder(ds.name, ds.streamType, newColumns)
                                .filtered(meta().verb, ds)
                                .build(ds.rdd.mapPartitionsToPair(it -> {
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
                }
                case Track: {
                    if (valueColumns == null) {
                        valueColumns = newColumns.get(OBJLVL_TRACK);
                    }

                    final List<String> segmentColumns = newColumns.get(OBJLVL_SEGMENT);
                    final List<String> pointColumns = newColumns.get(OBJLVL_POINT);

                    if ((valueColumns != null) || (segmentColumns != null) || (pointColumns != null)) {
                        final List<String> _newColumns = valueColumns;

                        return new DataStreamBuilder(ds.name, ds.streamType, newColumns)
                                .filtered(meta().verb, ds)
                                .build(ds.rdd.mapPartitionsToPair(it -> {
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
                }
                default: {
                    return new DataStreamBuilder(ds.name, ds.streamType, ds.accessor.attributes())
                            .passedthru(meta.verb, ds)
                            .build(ds.rdd);
                }
            }
        };
    }
}
