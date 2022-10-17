/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.*;

@SuppressWarnings("unused")
public class TrackStatsOperation extends Operation {
    public static final String INPUT_TRACKS = "tracks";
    public static final String INPUT_PINS = "pins";
    public static final String PINNING_MODE = "pinning_mode";
    static final String DEF_USERID = "_userid";
    private static final String PINS_USERID_PROP = "pins_userid_prop";
    private static final String TRACKS_USERID_PROP = "tracks_userid_prop";
    static final String GEN_POINTS = "_points";
    static final String GEN_DURATION = "_duration";
    static final String GEN_RADIUS = "_radius";
    static final String GEN_DISTANCE = "_distance";
    static final String GEN_AZI_TO_PREV = "_azi_to_prev";
    static final String GEN_AZI_FROM_PREV = "_azi_from_prev";
    static final String GEN_AZI_TO_NEXT = "_azi_to_next";
    static final String GEN_AZI_FROM_NEXT = "_azi_from_next";
    private static final String TRACKS_TS_PROP = "tracks_ts_prop";
    private static final String DEF_TS = "_ts";

    private String pinsUserid;
    private String tracksUserid;
    private String tracksTs;

    private PinningMode pinningMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("trackStats", "Take a Track DataStream and augment its Points', Segments'" +
                " and Tracks' properties with statistics",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(INPUT_TRACKS, "Track DataStream to calculate the statistics",
                                new StreamType[]{StreamType.Track}
                        )
                        .optionalInput(INPUT_PINS, "Optional Point DataStream to pin tracks with same User ID property against (for "
                                        + PINNING_MODE + "=" + PinningMode.INPUT_PINS + ")",
                                new StreamType[]{StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(PINNING_MODE, "Track pinning mode for radius calculation", PinningMode.class,
                                PinningMode.INPUT_PINS, "By default, pin to points supplied by an external input")
                        .def(PINS_USERID_PROP, "Column of User ID attribute of pins",
                                DEF_USERID, "By default, '" + DEF_USERID + "'")
                        .def(TRACKS_USERID_PROP, "Column of User ID attribute of tracks",
                                DEF_USERID, "By default, '" + DEF_USERID + "'")
                        .def(TRACKS_TS_PROP, "Timestamp property of track Point",
                                DEF_TS, "By default, '" + DEF_TS + "'")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("SegmentedTrack output RDD with stats",
                                new StreamType[]{StreamType.Track}, Origin.AUGMENTED, Arrays.asList(INPUT_TRACKS, INPUT_PINS)
                        )
                        .generated(GEN_POINTS, "Number of Track or Segment points")
                        .generated(GEN_DURATION, "Track or Segment duration, seconds")
                        .generated(GEN_RADIUS, "Track or Segment max distance from its pinning point, meters")
                        .generated(GEN_DISTANCE, "Track or Segment length, meters")
                        .generated(GEN_AZI_FROM_PREV, "Point azimuth from previous point")
                        .generated(GEN_AZI_TO_NEXT, "Point azimuth to next point")
                        .generated(GEN_AZI_TO_PREV, "Point azimuth to previous point")
                        .generated(GEN_AZI_FROM_NEXT, "Point azimuth from next point")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        pinsUserid = params.get(PINS_USERID_PROP);

        tracksUserid = params.get(TRACKS_USERID_PROP);
        tracksTs = params.get(TRACKS_TS_PROP);

        pinningMode = params.get(PINNING_MODE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        DataStream inputTracks = inputStreams.get(INPUT_TRACKS);

        JavaPairRDD<PointEx, SegmentedTrack> inp;
        if (pinningMode == PinningMode.INPUT_PINS) {
            final String _pinsUserid = pinsUserid;
            JavaPairRDD<String, PointEx> pins = ((JavaRDD<PointEx>) inputStreams.get(INPUT_PINS).get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, PointEx>> result = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx next = it.next();

                            result.add(new Tuple2<>(next.asString(_pinsUserid), next));
                        }

                        return result.iterator();
                    });

            final String _tracksUserid = tracksUserid;
            JavaPairRDD<String, SegmentedTrack> tracks = ((JavaRDD<SegmentedTrack>) inputTracks.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, SegmentedTrack>> result = new ArrayList<>();

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();

                            result.add(new Tuple2<>(next.asString(_tracksUserid), next));
                        }

                        return result.iterator();
                    });

            inp = pins.join(tracks)
                    .mapToPair(Tuple2::_2);
        } else {
            inp = ((JavaRDD<SegmentedTrack>) inputTracks.get())
                    .mapToPair(t -> new Tuple2<>(null, t));
        }

        final String _ts = tracksTs;
        final PinningMode _pinningMode = pinningMode;
        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<SegmentedTrack> output = inp
                .mapPartitions(it -> {
                    List<SegmentedTrack> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<PointEx, SegmentedTrack> o = it.next();

                        SegmentedTrack trk = o._2;

                        Point trkPin = null;
                        Point segPin = null;
                        int numSegs = trk.getNumGeometries();
                        TrackSegment[] segs = new TrackSegment[numSegs];
                        int augPoints = 0;
                        double augDistance = 0.D;
                        double augRadius = 0.D;
                        long augDuration = 0L;
                        for (int j = 0; j < numSegs; j++) {
                            TrackSegment augSeg;

                            TrackSegment seg = (TrackSegment) trk.getGeometryN(j);
                            Geometry[] wayPoints = seg.geometries();
                            int segPoints = wayPoints.length;
                            double segDistance = 0.D;
                            double segRadius = 0.D;
                            long segDuration = 0L;

                            switch (_pinningMode) {
                                case SEGMENT_CENTROIDS: {
                                    if (j == 0) {
                                        trkPin = trk.getCentroid();
                                    }
                                    segPin = seg.getCentroid();
                                    break;
                                }
                                case TRACK_CENTROIDS: {
                                    if (j == 0) {
                                        trkPin = trk.getCentroid();
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                                case SEGMENT_STARTS: {
                                    segPin = (Point) wayPoints[0];
                                    if (j == 0) {
                                        trkPin = segPin;
                                    }
                                    break;
                                }
                                case TRACK_STARTS: {
                                    if (j == 0) {
                                        trkPin = (Point) wayPoints[0];
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                                default: {
                                    if (j == 0) {
                                        trkPin = o._1;
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                            }

                            double pntRadius;
                            PointEx prev = (PointEx) wayPoints[0];
                            for (int i = 0; i < segPoints; i++) {
                                Geometry wayPoint = wayPoints[i];
                                PointEx point = (PointEx) wayPoint;

                                segDuration += point.asDouble(_ts) - prev.asDouble(_ts);
                                GeodesicData inverse = Geodesic.WGS84.Inverse(prev.getY(), prev.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE | GeodesicMask.AZIMUTH);
                                segDistance += inverse.s12;

                                if (i != 0) {
                                    point.put(GEN_AZI_FROM_PREV, inverse.azi2);
                                    point.put(GEN_AZI_TO_PREV, inverse.azi1);
                                    prev.put(GEN_AZI_FROM_NEXT, inverse.azi1);
                                    prev.put(GEN_AZI_TO_NEXT, inverse.azi2);
                                }

                                pntRadius = Geodesic.WGS84.Inverse(segPin.getY(), segPin.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12;
                                point.put(GEN_RADIUS, pntRadius);
                                segRadius = Math.max(segRadius, pntRadius);

                                augRadius = Math.max(augRadius, Geodesic.WGS84.Inverse(trkPin.getY(), trkPin.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12);

                                if ((_pinningMode == PinningMode.SEGMENT_CENTROIDS) || (_pinningMode == PinningMode.SEGMENT_STARTS)) {
                                    point.put(GEN_DURATION, segDuration);
                                    point.put(GEN_DISTANCE, segDistance);
                                    point.put(GEN_POINTS, i + 1);
                                } else {
                                    point.put(GEN_DURATION, augDuration + segDuration);
                                    point.put(GEN_DISTANCE, augDistance + segDistance);
                                    point.put(GEN_POINTS, augPoints + i + 1);
                                }

                                prev = point;
                            }

                            augSeg = new TrackSegment(wayPoints, geometryFactory);

                            augDuration += segDuration;
                            augDistance += segDistance;
                            augPoints += segPoints;

                            augSeg.setUserData(seg.getUserData());
                            augSeg.put(GEN_DURATION, segDuration);
                            augSeg.put(GEN_DISTANCE, segDistance);
                            augSeg.put(GEN_POINTS, segPoints);
                            augSeg.put(GEN_RADIUS, segRadius);

                            segs[j] = augSeg;
                        }

                        SegmentedTrack aug = new SegmentedTrack(segs, geometryFactory);
                        aug.setUserData(trk.getUserData());
                        aug.put(GEN_DURATION, augDuration);
                        aug.put(GEN_DISTANCE, augDistance);
                        aug.put(GEN_POINTS, augPoints);
                        aug.put(GEN_RADIUS, augRadius);

                        result.add(aug);
                    }

                    return result.iterator();
                });

        Map<String, List<String>> inColumns = inputTracks.accessor.attributes();
        Map<String, List<String>> outColumns = new HashMap<>();
        outColumns.put(OBJLVL_TRACK, inColumns.containsKey(OBJLVL_TRACK) ? new ArrayList<>(inColumns.get(OBJLVL_TRACK)) : new ArrayList<>());
        outColumns.get(OBJLVL_TRACK).add(GEN_POINTS);
        outColumns.get(OBJLVL_TRACK).add(GEN_RADIUS);
        outColumns.get(OBJLVL_TRACK).add(GEN_DISTANCE);
        outColumns.get(OBJLVL_TRACK).add(GEN_DURATION);
        outColumns.put(OBJLVL_SEGMENT, inColumns.containsKey(OBJLVL_SEGMENT) ? new ArrayList<>(inColumns.get(OBJLVL_SEGMENT)) : new ArrayList<>());
        outColumns.get(OBJLVL_SEGMENT).add(GEN_POINTS);
        outColumns.get(OBJLVL_SEGMENT).add(GEN_RADIUS);
        outColumns.get(OBJLVL_SEGMENT).add(GEN_DISTANCE);
        outColumns.get(OBJLVL_SEGMENT).add(GEN_DURATION);
        outColumns.put(OBJLVL_POINT, inColumns.containsKey(OBJLVL_POINT) ? new ArrayList<>(inColumns.get(OBJLVL_POINT)) : new ArrayList<>());
        outColumns.get(OBJLVL_POINT).add(GEN_AZI_FROM_NEXT);
        outColumns.get(OBJLVL_POINT).add(GEN_AZI_FROM_PREV);
        outColumns.get(OBJLVL_POINT).add(GEN_AZI_TO_NEXT);
        outColumns.get(OBJLVL_POINT).add(GEN_AZI_TO_PREV);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Track, output, outColumns));
    }

    public enum PinningMode implements DefinitionEnum {
        SEGMENT_CENTROIDS("Pin Segments by their own centroids"),
        TRACK_CENTROIDS("Pin Segments by parent Track centroid"),
        SEGMENT_STARTS("Pin Segments by their own starting points"),
        TRACK_STARTS("Pin Segments by parent Track starting point"),
        INPUT_PINS("Pin both Tracks and Segments by externally supplied pin points");

        private final String descr;

        PinningMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
