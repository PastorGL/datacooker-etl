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
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.WayPoint;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class TrackToGpxTransform implements Transform {
    static final String NAME_ATTR = "name_attr";
    static final String TIMESTAMP_ATTR = "ts_attr";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("trackToGpx", StreamType.Track, StreamType.PlainText,
                "Take a Track DataStream and produce a GPX fragment file",

                new DefinitionMetaBuilder()
                        .def(NAME_ATTR, "Attribute of Segmented Track that becomes GPX track name")
                        .def(TIMESTAMP_ATTR, "Attribute of Points that becomes GPX time stamp", null, "By default, don't set time stamp")
                        .build(),
                null

        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            String nameColumn = params.get(NAME_ATTR);
            String timeColumn = params.get(TIMESTAMP_ATTR);

            return new DataStream(StreamType.PlainText,
                    ((JavaRDD<SegmentedTrack>) ds.get()).mapPartitions(it -> {
                        List<Text> result = new ArrayList<>();

                        GPX.Writer writer = GPX.writer();

                        while (it.hasNext()) {
                            SegmentedTrack trk = it.next();

                            GPX.Builder gpx = GPX.builder();
                            gpx.creator("DataCooker");
                            Track.Builder trkBuilder = Track.builder();

                            for (Geometry g : trk) {
                                TrackSegment ts = (TrackSegment) g;
                                io.jenetics.jpx.TrackSegment.Builder segBuilder = io.jenetics.jpx.TrackSegment.builder();

                                for (Geometry gg : ts) {
                                    PointEx p = (PointEx) gg;
                                    WayPoint.Builder wp = WayPoint.builder();
                                    wp.lat(p.getY());
                                    wp.lon(p.getX());
                                    if (timeColumn != null) {
                                        wp.time(((Long) ((Map<String, Object>) p.getUserData()).get(timeColumn)));
                                    }

                                    segBuilder.addPoint(wp.build());
                                }

                                trkBuilder.addSegment(segBuilder.build());
                            }

                            if (nameColumn != null) {
                                trkBuilder.name(String.valueOf(((Map<String, Object>) trk.getUserData()).get(nameColumn)));
                            }
                            gpx.addTrack(trkBuilder.build());

                            result.add(new Text(writer.toString(gpx.build())));
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
