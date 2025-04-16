/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.WayPoint;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("unused")
public class GpxToTrackTransform extends Transformer {
    static final String USERID_ATTR = "userid_attr";
    static final String TIMESTAMP_ATTR = "ts_attr";
    static final String VERB = "gpxToTrack";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take Plain Text representation of GPX fragment file and produce a Track DataStream." +
                        " Does not preserve partitioning")
                .transform().operation()
                .input(StreamType.PLAIN_TEXT, "Input GPX DS")
                .output(StreamType.TRACK, "Output Track DS")
                .def(USERID_ATTR, "Name for the Track userid attribute derived from GPX trkType" +
                        " &lt;name&gt; element (or random UUID if absent)", "_userid", "By default, _userid")
                .def(TIMESTAMP_ATTR, "Name for the Point time stamp attribute derived from GPX wptType" +
                                " &lt;time&gt; element (or monotonously increasing number within track if absent)",
                        "_ts", "By default, _ts")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final CoordinateSequenceFactory csFactory = SpatialRecord.FACTORY.getCoordinateSequenceFactory();

            final String useridAttr = params.get(USERID_ATTR);
            final String tsAttr = params.get(TIMESTAMP_ATTR);

            return new DataStreamBuilder(outputName, newColumns)
                    .transformed(VERB, StreamType.Track, ds)
                    .build(ds.rdd().flatMapToPair(line -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        String l = String.valueOf(line._2);

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
                                    PointEx pt = new PointEx(csFactory.create(new Coordinate[]{new Coordinate(wp.getLongitude().doubleValue(), wp.getLatitude().doubleValue())}));

                                    Map<String, Object> props = new HashMap<>();
                                    props.put(tsAttr, (double) (wp.getTime().isPresent() ? wp.getTime().get().toEpochSecond() : _ts));
                                    pt.put(props);

                                    p[j] = pt;
                                    _ts++;
                                }

                                TrackSegment seg = new TrackSegment(p);

                                ts[i] = seg;
                            }

                            if (segmentsSize > 0) {
                                SegmentedTrack st = new SegmentedTrack(ts);

                                String name = g.getName().orElse(UUID.randomUUID().toString());
                                Map<String, Object> props = new HashMap<>();
                                props.put(useridAttr, name);
                                st.put(props);

                                ret.add(new Tuple2<>(line._1, st));
                            }
                        }

                        return ret.iterator();
                    }));
        };
    }
}
