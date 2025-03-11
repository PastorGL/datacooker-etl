/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.*;

@SuppressWarnings("unused")
public class TrackToPointTransform extends Transformer {

    static final String VERB = "trackToPoint";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Extracts all Points from Track DataStream")
                .transform(true).objLvls(TRACK, SEGMENT, POINT).operation()
                .input(StreamType.POINT, "Input Track DS")
                .output(StreamType.PLAIN_TEXT, "Output Point DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final List<String> _pointColumns = (newColumns != null) ? newColumns.get(POINT) : null;

            List<String> outColumns = new ArrayList<>();
            if (_pointColumns != null) {
                outColumns.addAll(_pointColumns);
            } else {
                outColumns.addAll(ds.attributes(TRACK));
                outColumns.addAll(ds.attributes(SEGMENT));
                outColumns.addAll(ds.attributes(POINT));
            }

            return new DataStreamBuilder(outputName, Collections.singletonMap(POINT, outColumns))
                    .transformed(VERB, StreamType.Point, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            SegmentedTrack trk = (SegmentedTrack) t._2;
                            for (Geometry g : trk.geometries()) {
                                TrackSegment s = (TrackSegment) g;

                                for (Geometry gg : s.geometries()) {
                                    Map<String, Object> pntProps = new HashMap<>(trk.asIs());
                                    pntProps.putAll(s.asIs());
                                    pntProps.putAll(((PointEx) gg).asIs());

                                    PointEx p = new PointEx(gg);
                                    if (_pointColumns != null) {
                                        for (String prop : _pointColumns) {
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
                    }, true));
        };
    }
}
