/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.*;
import io.github.pastorgl.datacooker.metadata.DescribedEnum;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
                                StreamType.POINT, StreamOrigin.GENERATED, null
                        )
                        .generated("*", "Properties from source Spatial objects are preserved")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        tracksMode = params.get(TRACKS_MODE);
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final TracksMode _tracksMode = tracksMode;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, DataRecord<?>> out = input.rdd()
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> r = it.next();

                            SpatialRecord<?> g = (SpatialRecord<?>) r._2;
                            if (g instanceof PolygonEx) {
                                PointEx centroid = ((PolygonEx) g).getCentroid();

                                centroid.put(g.asIs());
                                ret.add(new Tuple2<>(r._1, centroid));
                            } else {
                                if (_tracksMode != TracksMode.SEGMENTS) {
                                    PointEx centroid = ((SegmentedTrack) g).getCentroid();

                                    centroid.put(g.asIs());
                                    ret.add(new Tuple2<>(r._1, centroid));
                                }

                                if (_tracksMode != TracksMode.TRACKS) {
                                    for (Geometry gg : ((SegmentedTrack) g).geometries()) {
                                        PointEx centroid = ((TrackSegment) gg).getCentroid();

                                        HashMap<String, Object> props = new HashMap<>(g.asIs());
                                        props.putAll(((TrackSegment) gg).asIs());

                                        centroid.put(props);
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
                    outputColumns = input.attributes(ObjLvl.POINT);
                    break;
                }
                case Track: {
                    switch (tracksMode) {
                        case SEGMENTS: {
                            outputColumns = input.attributes(ObjLvl.SEGMENT);
                            break;
                        }
                        case TRACKS: {
                            outputColumns = input.attributes(ObjLvl.TRACK);
                            break;
                        }
                        default: {
                            outputColumns = new ArrayList<>(input.attributes(ObjLvl.TRACK));
                            outputColumns.addAll(input.attributes(ObjLvl.SEGMENT));
                        }
                    }
                    break;
                }
                case Polygon: {
                    outputColumns = input.attributes(ObjLvl.POLYGON);
                    break;
                }
            }

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), Collections.singletonMap(ObjLvl.POINT, outputColumns))
                    .generated(meta.verb, StreamType.Point, input)
                    .build(out)
            );
        }

        return outputs;
    }

    public enum TracksMode implements DescribedEnum {
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
