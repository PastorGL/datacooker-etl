/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamConverter;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.Transform;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.*;

@SuppressWarnings("unused")
public class TrackToPointTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("trackToPoint", StreamType.Track, StreamType.Point,
                "Extracts all Points from SegmentedTrack DataStream",

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

            return new DataStream(StreamType.Point,
                    ((JavaRDD<SegmentedTrack>) ds.get()).mapPartitions(it -> {
                        List<PointEx> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();
                            Map<String, Object> trkData = (Map) next.getUserData();

                            for (Geometry g : next.geometries()) {
                                TrackSegment s = (TrackSegment) g;
                                Map<String, Object> segData = (Map) g.getUserData();

                                for (Geometry gg : s.geometries()) {
                                    Map<String, Object> pntProps = new HashMap<>(trkData);
                                    pntProps.putAll(segData);
                                    pntProps.putAll((Map) gg.getUserData());

                                    PointEx p = new PointEx(gg);
                                    if (_outputColumns != null) {
                                        for (String prop : _outputColumns) {
                                            p.put(prop, pntProps.get(prop));
                                        }
                                    } else {
                                        p.put(pntProps);
                                    }

                                    ret.add(p);
                                }
                            }
                        }

                        return ret.iterator();
                    }), Collections.singletonMap(OBJLVL_POINT, outColumns));
        };
    }
}
