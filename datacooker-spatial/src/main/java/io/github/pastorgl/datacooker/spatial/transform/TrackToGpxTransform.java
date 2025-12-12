/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.WayPoint;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class TrackToGpxTransform extends Transformer {
    static final String NAME_ATTR = "name_attr";
    static final String TIMESTAMP_ATTR = "ts_attr";
    static final String VERB = "trackToGpx";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take a Track DataStream and produce a GPX fragment file.")
                .transform().operation()
                .input(StreamType.TRACK, "Input Track DS")
                .output(StreamType.PLAIN_TEXT, "Output GPX DS")
                .def(NAME_ATTR, "Attribute of Segmented Track that becomes GPX track name")
                .def(TIMESTAMP_ATTR, "Attribute of Points that becomes GPX time stamp",
                        null, "By default, don't set time stamp")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final String name = params.get(NAME_ATTR);
            final String time = params.get(TIMESTAMP_ATTR);

            return new DataStreamBuilder(outputName, null)
                    .transformed(VERB, StreamType.PlainText, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        GPX.Writer writer = GPX.writer();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            GPX.Builder gpx = GPX.builder();
                            gpx.creator("DataCooker");
                            Track.Builder trkBuilder = Track.builder();

                            SegmentedTrack trk = (SegmentedTrack) t._2;
                            for (Geometry g : trk) {
                                TrackSegment ts = (TrackSegment) g;
                                io.jenetics.jpx.TrackSegment.Builder segBuilder = io.jenetics.jpx.TrackSegment.builder();

                                for (Geometry gg : ts) {
                                    PointEx p = (PointEx) gg;
                                    WayPoint.Builder wp = WayPoint.builder();
                                    wp.lat(p.getY());
                                    wp.lon(p.getX());
                                    if (time != null) {
                                        wp.time(p.asLong(time));
                                    }

                                    segBuilder.addPoint(wp.build());
                                }

                                trkBuilder.addSegment(segBuilder.build());
                            }

                            if (name != null) {
                                trkBuilder.name(trk.asString(name));
                            }
                            gpx.addTrack(trkBuilder.build());

                            ret.add(new Tuple2<>(t._1, new PlainText(writer.toString(gpx.build()))));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
