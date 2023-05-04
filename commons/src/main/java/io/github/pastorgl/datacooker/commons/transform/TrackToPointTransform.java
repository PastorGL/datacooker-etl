/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unused")
public class TrackToPointTransform extends Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("trackToPoint", StreamType.Track, StreamType.Point,
                "Extracts all Points from Track DataStream",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get(OBJLVL_POINT);

            List<String> outColumns = new ArrayList<>();
            if (_outputColumns != null) {
                outColumns.addAll(_outputColumns);
            } else {
                outColumns.addAll(ds.accessor.attributes(OBJLVL_TRACK));
                outColumns.addAll(ds.accessor.attributes(OBJLVL_SEGMENT));
                outColumns.addAll(ds.accessor.attributes(OBJLVL_POINT));
            }

            return new DataStream(StreamType.Point, ds.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> t = it.next();

                            SegmentedTrack trk = (SegmentedTrack) t._2;
                            for (Geometry g : trk.geometries()) {
                                TrackSegment s = (TrackSegment) g;

                                for (Geometry gg : s.geometries()) {
                                    Map<String, Object> pntProps = new HashMap<>(trk.asIs());
                                    pntProps.putAll(s.asIs());
                                    pntProps.putAll((Map<String, Object>) gg.getUserData());

                                    PointEx p = new PointEx(gg);
                                    if (_outputColumns != null) {
                                        for (String prop : _outputColumns) {
                                            p.put(prop, pntProps.get(prop));
                                        }
                                    } else {
                                        p.put(pntProps);
                                    }

                                    ret.add(new Tuple2<>(t._1, p));
                                }
                            }
                        }

                        return ret.iterator();
                    }, true), Collections.singletonMap(OBJLVL_POINT, outColumns));
        };
    }
}
