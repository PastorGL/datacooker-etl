/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import io.github.pastorgl.datacooker.spatial.utils.TrackComparator;
import io.github.pastorgl.datacooker.spatial.utils.TrackPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;
import scala.Tuple4;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unused")
public class ColumnarToTrackTransform extends Transform {
    static final String LAT_COLUMN = "lat_column";
    static final String LON_COLUMN = "lon_column";
    static final String TS_COLUMN = "ts_column";
    static final String USERID_COLUMN = "userid_column";
    static final String TRACKID_COLUMN = "trackid_column";
    static final String GEN_USERID = "_userid";
    static final String GEN_TRACKID = "_trackid";
    static final String GEN_TIMESTAMP = "_ts";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToTrack", StreamType.Columnar, StreamType.Track,
                "Transform Columnar DataStream to Track using record columns. Does not preserve partitioning",

                new DefinitionMetaBuilder()
                        .def(LAT_COLUMN, "Point latitude column")
                        .def(LON_COLUMN, "Point longitude column")
                        .def(TS_COLUMN, "Point time stamp column")
                        .def(USERID_COLUMN, "Point User ID column")
                        .def(TRACKID_COLUMN, "Optional Point track segment ID column",
                                null, "By default, create single-segmented tracks")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_USERID, "User ID property of Tracks and Segments")
                        .genCol(GEN_TRACKID, "Track ID property of Segmented Tracks")
                        .genCol(GEN_TIMESTAMP, "Time stamp of a Point")
                        .build()

        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final String _latColumn = params.get(LAT_COLUMN);
            final String _lonColumn = params.get(LON_COLUMN);
            final String _tsColumn = params.get(TS_COLUMN);
            final String _useridColumn = params.get(USERID_COLUMN);
            final String _trackColumn = params.get(TRACKID_COLUMN);

            List<String> pointColumns = newColumns.get(OBJLVL_POINT);
            if (pointColumns == null) {
                pointColumns = ds.accessor.attributes(OBJLVL_VALUE);
            }
            final List<String> _pointColumns = pointColumns;

            JavaPairRDD<Object, Record<?>> signalsInput = ds.rdd;
            int _numPartitions = signalsInput.getNumPartitions();

            final boolean isSegmented = (_trackColumn != null);

            JavaPairRDD<Tuple2<String, Double>, Tuple4<Double, Double, String, Record<?>>> signals = signalsInput
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Tuple2<String, Double>, Tuple4<Double, Double, String, Record<?>>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> row = it.next();

                            String userId = row._2.asString(_useridColumn);
                            Double lat = row._2.asDouble(_latColumn);
                            Double lon = row._2.asDouble(_lonColumn);
                            Double timestamp = row._2.asDouble(_tsColumn);

                            String track = isSegmented ? row._2.asString(_trackColumn) : null;

                            ret.add(new Tuple2<>(new Tuple2<>(userId, timestamp), new Tuple4<>(lat, lon, track, row._2)));
                        }

                        return ret.iterator();
                    })
                    .repartitionAndSortWithinPartitions(new TrackPartitioner(_numPartitions), new TrackComparator<>()) // pre-sort by timestamp
                    ;

            HashMap<Integer, Integer> useridCountPerPartition = new HashMap<>(signals
                    .mapPartitionsWithIndex((idx, it) -> {
                        List<Tuple2<Integer, Integer>> num = new ArrayList<>();

                        Set<String> userids = new HashSet<>();
                        while (it.hasNext()) {
                            String userid = it.next()._1._1;
                            userids.add(userid);
                        }

                        num.add(new Tuple2<>(idx, userids.size()));

                        return num.iterator();
                    }, true)
                    .mapToPair(t -> t)
                    .collectAsMap()
            );

            Broadcast<HashMap<Integer, Integer>> num = JavaSparkContext.fromSparkContext(signalsInput.context()).broadcast(useridCountPerPartition);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaPairRDD<Object, Record<?>> output = signals
                    .mapPartitionsWithIndex((idx, it) -> {
                        int useridCount = num.getValue().get(idx);

                        Map<String, Integer> useridOrd = new HashMap<>();

                        String[] userids = new String[useridCount];
                        List<Map<String, Object>>[] allSegProps = new List[useridCount];
                        List<List<PointEx>>[] allPoints = new List[useridCount];
                        int n = 0;
                        while (it.hasNext()) {
                            Tuple2<Tuple2<String, Double>, Tuple4<Double, Double, String, Record<?>>> line = it.next();

                            String userid = line._1._1;
                            int current;
                            if (useridOrd.containsKey(userid)) {
                                current = useridOrd.get(userid);
                            } else {
                                useridOrd.put(userid, n);
                                userids[n] = userid;
                                current = n;

                                n++;
                            }

                            List<Map<String, Object>> segProps = allSegProps[current];
                            List<List<PointEx>> trackPoints = allPoints[current];
                            if (segProps == null) {
                                segProps = new ArrayList<>();
                                allSegProps[current] = segProps;
                                trackPoints = new ArrayList<>();
                                allPoints[current] = trackPoints;
                            }

                            List<PointEx> segPoints;
                            String trackId;
                            if (isSegmented) {
                                trackId = line._2._3();

                                String lastTrackId = null;
                                Map<String, Object> lastSegment;
                                if (segProps.size() != 0) {
                                    lastSegment = segProps.get(segProps.size() - 1);
                                    lastTrackId = lastSegment.get(GEN_TRACKID).toString();
                                }

                                if (trackId.equals(lastTrackId)) {
                                    segPoints = trackPoints.get(trackPoints.size() - 1);
                                } else {
                                    Map<String, Object> props = new HashMap<>();
                                    props.put(GEN_USERID, userid);
                                    props.put(GEN_TRACKID, trackId);

                                    segProps.add(props);
                                    segPoints = new ArrayList<>();
                                    trackPoints.add(segPoints);
                                }
                            } else {
                                if (segProps.size() == 0) {
                                    Map<String, Object> props = new HashMap<>();
                                    props.put(GEN_USERID, userid);

                                    segProps.add(props);
                                    segPoints = new ArrayList<>();
                                    trackPoints.add(segPoints);
                                } else {
                                    segPoints = trackPoints.get(0);
                                }
                            }

                            PointEx point = new PointEx(geometryFactory.createPoint(new Coordinate(line._2._2(), line._2._1())));
                            Map<String, Object> pointProps = new HashMap<>();
                            Record<?> row = line._2._4();
                            for (String col : _pointColumns) {
                                pointProps.put(col, row.asIs(col));
                            }
                            pointProps.put(GEN_TIMESTAMP, line._1._2);
                            point.setUserData(pointProps);

                            segPoints.add(point);
                        }

                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>(useridCount);

                        for (n = 0; n < useridCount; n++) {
                            String userid = userids[n];

                            List<List<PointEx>> points = allPoints[n];
                            TrackSegment[] segments = new TrackSegment[points.size()];
                            for (int i = 0; i < points.size(); i++) {
                                List<PointEx> segPoints = points.get(i);
                                segments[i] = new TrackSegment(segPoints.toArray(new PointEx[0]), geometryFactory);
                                segments[i].setUserData(allSegProps[n].get(i));
                            }

                            SegmentedTrack trk = new SegmentedTrack(segments, geometryFactory);

                            Map<String, Object> props = new HashMap<>();
                            props.put(GEN_USERID, userid);
                            trk.setUserData(props);

                            ret.add(new Tuple2<>(userid, trk));
                        }

                        return ret.iterator();
                    }, true)
                    .mapToPair(t -> t);

            Map<String, List<String>> outputColumns = new HashMap<>();
            outputColumns.put(OBJLVL_TRACK, Collections.singletonList(GEN_USERID));
            List<String> segmentProps = new ArrayList<>();
            segmentProps.add(GEN_USERID);
            if (isSegmented) {
                segmentProps.add(GEN_TRACKID);
            }
            outputColumns.put(OBJLVL_SEGMENT, segmentProps);
            List<String> pointProps = new ArrayList<>(_pointColumns);
            pointProps.add(GEN_TIMESTAMP);
            outputColumns.put(OBJLVL_POINT, pointProps);

            return new DataStream(StreamType.Track, output, outputColumns);
        };
    }
}
