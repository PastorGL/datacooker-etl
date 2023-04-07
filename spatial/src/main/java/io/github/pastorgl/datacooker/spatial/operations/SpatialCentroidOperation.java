/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Operation {
    public static final String TRACKS_MODE = "tracks_mode";

    private TracksMode tracksMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("spatialCentroid", "Take a Track or Polygon DataStream and extract a Point DataStream" +
                " of centroids while keeping all other properties",

                new PositionalStreamsMetaBuilder()
                        .input("Source Track or Polygon DataStream",
                                new StreamType[]{StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(TRACKS_MODE, "What to output for Track DataStreams", TracksMode.class,
                                TracksMode.BOTH, "By default, output both Tracks' and Segments' data")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("POI DataStream (Points with centroids, and each has radius set)",
                                new StreamType[]{StreamType.Point}, Origin.GENERATED, null
                        )
                        .generated("*", "Properties from source spatial objects are preserved")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        tracksMode = params.get(TRACKS_MODE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final TracksMode _tracksMode = tracksMode;

        DataStream input = inputStreams.getValue(0);

        JavaRDD<PointEx> output = ((JavaRDD<Object>) input.get())
                .mapPartitions(it -> {
                    List<PointEx> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Geometry g = (Geometry) it.next();

                        if (g instanceof PolygonEx) {
                            PointEx centroid = ((PolygonEx) g).getCentroid();

                            HashMap<String, Object> props = new HashMap<>((HashMap<String, Object>) g.getUserData());

                            centroid.setUserData(props);
                            ret.add(centroid);
                        } else {
                            HashMap<String, Object> trackProps = (HashMap<String, Object>) g.getUserData();

                            if (_tracksMode != TracksMode.SEGMENTS) {
                                PointEx centroid = ((SegmentedTrack) g).centrePoint;

                                HashMap<String, Object> props = new HashMap<>(trackProps);

                                centroid.setUserData(props);
                                ret.add(centroid);
                            }

                            if (_tracksMode != TracksMode.TRACKS) {
                                for (Geometry gg : ((SegmentedTrack) g).geometries()) {
                                    PointEx centroid = ((TrackSegment) gg).getCentroid();

                                    HashMap<String, Object> props = new HashMap<>(trackProps);
                                    props.putAll((HashMap<String, Object>) gg.getUserData());

                                    centroid.setUserData(props);
                                    ret.add(centroid);
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

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Point, output, Collections.singletonMap(OBJLVL_POINT, outputColumns)));
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
