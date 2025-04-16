/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.*;
import io.github.pastorgl.datacooker.metadata.DescribedEnum;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Transformer {
    public static final String TRACKS_MODE = "tracks_mode";
    static final String VERB = "spatialCentroid";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Take DataStreams and extract Point DataStreams" +
                " of centroids while keeping all other properties")
                .operation().transform()
                .input(StreamType.SPATIAL, "Source Spatial DataStream")
                .def(TRACKS_MODE, "What to output for Track DataStreams", TracksMode.class,
                        TracksMode.BOTH, "By default, output both Tracks' and Segments' data")
                .output(StreamType.POINT, "POI DataStream (Points of centroids, and each has radius set)",
                        StreamOrigin.GENERATED, null)
                .generated("*", "Properties from source Spatial objects are preserved")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (input, ignore, params) -> {
            final TracksMode _tracksMode = params.get(TRACKS_MODE);
            final String _name = outputName;

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
                    switch (_tracksMode) {
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

            return new DataStreamBuilder(_name, Collections.singletonMap(ObjLvl.POINT, outputColumns))
                    .generated(VERB, StreamType.Point, input)
                    .build(out);
        };
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
