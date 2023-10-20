/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;

@SuppressWarnings("unused")
public class AreaCoversOperation extends Operation {
    static final String INPUT_POINTS = "points";
    static final String INPUT_POLYGONS = "polygons";
    static final String OUTPUT_TARGET = "target";
    static final String OUTPUT_EVICTED = "evicted";

    static final String ENCOUNTER_MODE = "encounter_mode";

    private EncounterMode once;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("areaCovers", "Take a Spatial and Polygon DataStreams and generate a DataStream consisting" +
                " of all Spatial objects that have centroids (signals) contained inside the Polygons. Optionally, it can emit signals" +
                " outside of all Polygons. Polygon sizes should be considerably small, i.e. few hundred meters at most",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(INPUT_POINTS, "Source Spatial objects with signals",
                                StreamType.SPATIAL
                        )
                        .mandatoryInput(INPUT_POLYGONS, "Source Polygons",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(ENCOUNTER_MODE, "This flag regulates creation of copies of a signal for each overlapping geometry",
                                EncounterMode.class, EncounterMode.COPY, "By default, create a distinct copy of a signal for each area it encounters inside")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .mandatoryOutput(OUTPUT_TARGET, "Output Point DataStream with fenced signals",
                                StreamType.SPATIAL, StreamOrigin.AUGMENTED, Arrays.asList(INPUT_POINTS, INPUT_POLYGONS)
                        )
                        .generated(OUTPUT_TARGET, "*", "Points will be augmented with Polygon properties")
                        .optionalOutput(OUTPUT_EVICTED, "Optional output Point DataStream with evicted signals",
                                StreamType.SPATIAL, StreamOrigin.FILTERED, Collections.singletonList(INPUT_POINTS)
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        once = params.get(ENCOUNTER_MODE);
    }

    @Override
    public Map<String, DataStream> execute() {
        EncounterMode _once = once;

        DataStream inputGeometries = inputStreams.get(INPUT_POLYGONS);
        JavaPairRDD<Object, Record<?>> geometriesInput = inputGeometries.rdd;

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
        JavaPairRDD<Object, Record<?>> signalsInput = inputSignals.rdd;

        Map<Long, Iterable<PolygonEx>> hashedGeometriesMap = hashedGeometries
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed polys
        Broadcast<HashMap<Long, Iterable<PolygonEx>>> broadcastHashedGeometries = JavaSparkContext.fromSparkContext(signalsInput.context())
                .broadcast(new HashMap<>(hashedGeometriesMap));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Object, Tuple2<Boolean, Record<?>>> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<PolygonEx>> geometries = broadcastHashedGeometries.getValue();

                    List<Tuple2<Object, Tuple2<Boolean, Record<?>>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> signal = it.next();
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

        Map<String, DataStream> ret = new HashMap<>();
        List<String> outputColumns = new ArrayList<>(inputSignals.accessor.attributes(OBJLVL_POINT));
        outputColumns.addAll(inputGeometries.accessor.attributes(OBJLVL_POLYGON));
        ret.put(outputStreams.get(OUTPUT_TARGET), new DataStreamBuilder(outputStreams.get(OUTPUT_TARGET), inputSignals.streamType, Collections.singletonMap(OBJLVL_POINT, outputColumns))
                .augmented(meta.verb, inputSignals, inputGeometries)
                .build(signals
                        .filter(t -> t._2._1)
                        .mapToPair(t -> new Tuple2<>(t._1, t._2._2)))
        );

        String outputEvictedName = outputStreams.get(OUTPUT_EVICTED);
        if (outputEvictedName != null) {
            ret.put(outputEvictedName, new DataStreamBuilder(outputEvictedName, inputSignals.streamType, Collections.singletonMap(OBJLVL_POINT, inputSignals.accessor.attributes(OBJLVL_POINT)))
                    .filtered(meta.verb, inputSignals)
                    .build(signals
                            .filter(t -> !t._2._1)
                            .mapToPair(t -> new Tuple2<>(t._1, t._2._2)))
            );
        }

        return Collections.unmodifiableMap(ret);
    }

    private enum EncounterMode implements DefinitionEnum {
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
