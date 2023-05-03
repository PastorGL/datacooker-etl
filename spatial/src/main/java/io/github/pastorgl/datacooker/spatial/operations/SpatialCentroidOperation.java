/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.*;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Operation {
    public static final String TRACKS_MODE = "tracks_mode";

    private TracksMode tracksMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("spatialCentroid", "Take DataStreams and extract Point DataStreams" +
                " of centroids while keeping all other properties",

                new PositionalStreamsMetaBuilder()
                        .input("Source Spatial DataStream",
                                StreamType.SPATIAL
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(TRACKS_MODE, "What to output for Track DataStreams", TracksMode.class,
                                TracksMode.BOTH, "By default, output both Tracks' and Segments' data")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("POI DataStream (Points of centroids, and each has radius set)",
                                new StreamType[]{StreamType.Point}, Origin.GENERATED, null
                        )
                        .generated("*", "Properties from source Spatial objects are preserved")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        tracksMode = params.get(TRACKS_MODE);
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final TracksMode _tracksMode = tracksMode;

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, Record<?>> out = input.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> r = it.next();

                            SpatialRecord<?> g = (SpatialRecord<?>) r._2;
                            if (g instanceof PolygonEx) {
                                PointEx centroid = ((PolygonEx) g).getCentroid();

                                centroid.setUserData(g.asIs());
                                ret.add(new Tuple2<>(r._1, centroid));
                            } else {
                                if (_tracksMode != TracksMode.SEGMENTS) {
                                    PointEx centroid = ((SegmentedTrack) g).getCentroid();

                                    centroid.setUserData(g.asIs());
                                    ret.add(new Tuple2<>(r._1, centroid));
                                }

                                if (_tracksMode != TracksMode.TRACKS) {
                                    for (Geometry gg : ((SegmentedTrack) g).geometries()) {
                                        PointEx centroid = ((TrackSegment) gg).getCentroid();

                                        HashMap<String, Object> props = new HashMap<>(g.asIs());
                                        props.putAll(((TrackSegment) gg).asIs());

                                        centroid.setUserData(props);
                                        ret.add(new Tuple2<>(r._1, centroid));
                                    }
                                }
                            }
                        }

                        return ret.iterator();
                    });

            List<String> outputColumns = null;
            switch (input.streamType) {
                case Point: {
                    outputColumns = input.accessor.attributes(OBJLVL_POINT);
                    break;
                }
                case Track: {
                    switch (tracksMode) {
                        case SEGMENTS: {
                            outputColumns = input.accessor.attributes(OBJLVL_SEGMENT);
                            break;
                        }
                        case TRACKS: {
                            outputColumns = input.accessor.attributes(OBJLVL_TRACK);
                            break;
                        }
                        default: {
                            outputColumns = new ArrayList<>(input.accessor.attributes(OBJLVL_TRACK));
                            outputColumns.addAll(input.accessor.attributes(OBJLVL_SEGMENT));
                        }
                    }
                    break;
                }
                case Polygon: {
                    outputColumns = input.accessor.attributes(OBJLVL_POLYGON);
                    break;
                }
            }

            output.put(outputStreams.get(i), new DataStream(StreamType.Point, out, Collections.singletonMap(OBJLVL_POINT, outputColumns)));
        }

        return output;
    }

    public enum TracksMode implements DefinitionEnum {
        SEGMENTS("Output only Tracks' centroids"),
        TRACKS("Output only Segments' centroids"),
        BOTH("Output both Tracks' and then each of their Segments' centroids");

        private final String descr;

        TracksMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
