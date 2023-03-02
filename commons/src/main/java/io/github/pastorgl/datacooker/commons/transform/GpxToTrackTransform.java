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
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("unused")
public class GpxToTrackTransform implements Transform {
    static final String USERID_ATTR = "userid_attr";
    static final String TIMESTAMP_ATTR = "ts_attr";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("gpxToTrack", StreamType.PlainText, StreamType.Track,
                "Take Plain Text representation of GPX fragment file and produce a Track DataStream",

                new DefinitionMetaBuilder()
                        .def(USERID_ATTR, "Name for the Track userid attribute derived from GPX trkType &lt;name&gt; element (or random UUID if absent)", "_userid", "By default, _userid")
                        .def(TIMESTAMP_ATTR, "Name for the Point time stamp attribute derived from GPX wptType &lt;time&gt; element (or monotonously increasing number within track if absent)", "_ts", "By default, _ts")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final GeometryFactory geometryFactory = new GeometryFactory();
            final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

            final String useridAttr = params.get(USERID_ATTR);
            final String tsAttr = params.get(TIMESTAMP_ATTR);

            return new DataStream(StreamType.Track, ((JavaRDD<Object>) ds.get())
                    .flatMap(line -> {
                        List<SegmentedTrack> result = new ArrayList<>();

                        String l = String.valueOf(line);

                        GPX gpx = GPX.reader(GPX.Reader.Mode.LENIENT).read(new ByteArrayInputStream(l.getBytes(StandardCharsets.UTF_8)));

                        for (Track g : gpx.getTracks()) {
                            List<io.jenetics.jpx.TrackSegment> segments = g.getSegments();
                            int segmentsSize = segments.size();
                            TrackSegment[] ts = new TrackSegment[segmentsSize];

                            int _ts = 0;
                            for (int i = 0; i < segmentsSize; i++) {
                                io.jenetics.jpx.TrackSegment t = segments.get(i);

                                List<WayPoint> points = t.getPoints();
                                int pointsSize = points.size();
                                PointEx[] p = new PointEx[pointsSize];

                                for (int j = 0; j < pointsSize; j++) {
                                    WayPoint wp = points.get(j);
                                    PointEx pt = new PointEx(csFactory.create(new Coordinate[]{new Coordinate(wp.getLongitude().doubleValue(), wp.getLatitude().doubleValue())}), geometryFactory);

                                    Map<String, Object> props = new HashMap<>();
                                    props.put(tsAttr, (double) (wp.getTime().isPresent() ? wp.getTime().get().toEpochSecond() : _ts));
                                    pt.setUserData(props);

                                    p[j] = pt;
                                    _ts++;
                                }

                                TrackSegment seg = new TrackSegment(p, geometryFactory);

                                ts[i] = seg;
                            }

                            if (segmentsSize > 0) {
                                SegmentedTrack st = new SegmentedTrack(ts, geometryFactory);

                                String name = g.getName().orElse(UUID.randomUUID().toString());
                                Map<String, Object> props = new HashMap<>();
                                props.put(useridAttr, name);
                                st.setUserData(props);

                                result.add(st);
                            }
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
