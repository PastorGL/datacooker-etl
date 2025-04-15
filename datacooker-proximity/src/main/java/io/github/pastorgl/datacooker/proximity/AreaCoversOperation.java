/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.DescribedEnum;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.FullOperation;
import io.github.pastorgl.datacooker.data.spatial.SpatialUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;
import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;

@SuppressWarnings("unused")
public class AreaCoversOperation extends FullOperation {
    static final String INPUT_POINTS = "points";
    static final String INPUT_POLYGONS = "polygons";
    static final String OUTPUT_TARGET = "target";
    static final String OUTPUT_EVICTED = "evicted";

    static final String ENCOUNTER_MODE = "encounter_mode";
    static final String VERB = "areaCovers";

    private EncounterMode once;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Take a Spatial and Polygon DataStreams and generate a DataStream consisting" +
                " of all Spatial objects that have centroids (signals) contained inside the Polygons. Optionally, it can emit signals" +
                " outside of all Polygons. Polygon sizes should be considerably small, i.e. few hundred meters at most")
                .operation()
                .input(INPUT_POINTS, StreamType.SPATIAL, "Source Spatial objects with signals")
                .input(INPUT_POLYGONS, StreamType.POLYGON, "Source Polygons")
                .def(ENCOUNTER_MODE, "This flag regulates creation of copies of a signal for each overlapping geometry",
                        EncounterMode.class, EncounterMode.COPY, "By default, create a distinct copy of a signal for each area it encounters inside")
                .output(OUTPUT_TARGET, StreamType.SPATIAL, "Output Point DataStream with fenced signals",
                        StreamOrigin.AUGMENTED, Arrays.asList(INPUT_POINTS, INPUT_POLYGONS)
                )
                .generated(OUTPUT_TARGET, "*", "Points will be augmented with Polygon properties")
                .optOutput(OUTPUT_EVICTED, StreamType.SPATIAL, "Optional output Point DataStream with evicted signals",
                        StreamOrigin.FILTERED, Collections.singletonList(INPUT_POINTS)
                )
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        once = params.get(ENCOUNTER_MODE);
    }

    @Override
    public void execute() {
        EncounterMode _once = once;

        DataStream inputGeometries = inputStreams.get(INPUT_POLYGONS);
        JavaPairRDD<Object, DataRecord<?>> geometriesInput = inputGeometries.rdd();

        final double maxRadius = geometriesInput
                .mapToDouble(poly -> ((PolygonEx) poly._2).getCentroid().getRadius())
                .max(Comparator.naturalOrder());

        final SpatialUtils spatialUtils = new SpatialUtils(maxRadius);

        JavaPairRDD<Long, PolygonEx> hashedGeometries = geometriesInput
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, PolygonEx>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        PolygonEx o = (PolygonEx) it.next()._2;

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(o.getCentroid().getY(), o.getCentroid().getX()), o)
                        );
                    }

                    return result.iterator();
                });

        DataStream inputSignals = inputStreams.get(INPUT_POINTS);
        JavaPairRDD<Object, DataRecord<?>> signalsInput = inputSignals.rdd();

        Map<Long, Iterable<PolygonEx>> hashedGeometriesMap = hashedGeometries
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed polys
        Broadcast<HashMap<Long, Iterable<PolygonEx>>> broadcastHashedGeometries = JavaSparkContext.fromSparkContext(signalsInput.context())
                .broadcast(new HashMap<>(hashedGeometriesMap));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Object, Tuple2<Boolean, DataRecord<?>>> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<PolygonEx>> geometries = broadcastHashedGeometries.getValue();

                    List<Tuple2<Object, Tuple2<Boolean, DataRecord<?>>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> signal = it.next();
                        boolean added = false;

                        Point centroid = ((SpatialRecord<?>) signal._2).getCentroid();
                        double signalLat = centroid.getY();
                        double signalLon = centroid.getX();
                        List<Long> neighood = spatialUtils.getNeighbours(signalLat, signalLon);

                        once:
                        for (Long hash : neighood) {
                            if (geometries.containsKey(hash)) {
                                for (PolygonEx geometry : geometries.get(hash)) {
                                    if (centroid.within(geometry)) {
                                        if (_once == EncounterMode.ONCE) {
                                            result.add(new Tuple2<>(signal._1, new Tuple2<>(true, signal._2)));
                                        } else {
                                            SpatialRecord<?> point = (SpatialRecord<?>) signal._2.clone();
                                            point.put(geometry.asIs());
                                            point.put(signal._2.asIs());
                                            result.add(new Tuple2<>(signal._1, new Tuple2<>(true, point)));
                                        }
                                        added = true;
                                    }

                                    if ((_once == EncounterMode.COPY) && added) {
                                        break once;
                                    }
                                }
                            }
                        }

                        if (!added) {
                            result.add(new Tuple2<>(signal._1, new Tuple2<>(false, signal._2)));
                        }
                    }

                    return result.iterator();
                });

        List<String> outputColumns = new ArrayList<>(inputSignals.attributes(POINT));
        outputColumns.addAll(inputGeometries.attributes(POLYGON));
        outputs.put(OUTPUT_TARGET, new DataStreamBuilder(outputStreams.get(OUTPUT_TARGET), Collections.singletonMap(POINT, outputColumns))
                .augmented(VERB, inputSignals, inputGeometries)
                .build(signals
                        .filter(t -> t._2._1)
                        .mapToPair(t -> new Tuple2<>(t._1, t._2._2)))
        );

        String outputEvictedName = outputStreams.get(OUTPUT_EVICTED);
        if (outputEvictedName != null) {
            outputs.put(OUTPUT_EVICTED, new DataStreamBuilder(outputEvictedName, Collections.singletonMap(POINT, inputSignals.attributes(POINT)))
                    .filtered(VERB, inputSignals)
                    .build(signals
                            .filter(t -> !t._2._1)
                            .mapToPair(t -> new Tuple2<>(t._1, t._2._2)))
            );
        }
    }

    private enum EncounterMode implements DescribedEnum {
        ONCE("This flag suppresses creation of copies of a signal for each overlapping Polygon." +
                " Properties of the source signal will be unchanged"),
        COPY("For this flag, a distinct copy of source signal will be created for each overlapping Polygon," +
                " and their properties will be augmented with properties of that Polygon");

        private final String descr;

        EncounterMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
