/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POINT;

@SuppressWarnings("unused")
public class ProximityOperation extends Operation {
    static final String INPUT_POINTS = "points";
    static final String INPUT_POIS = "pois";
    static final String OUTPUT_TARGET = "target";
    static final String OUTPUT_EVICTED = "evicted";

    static final String ENCOUNTER_MODE = "encounter_mode";

    static final String GEN_DISTANCE = "_distance";

    private EncounterMode once;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("proximity", "Take a Spatial DataStream and POI DataStream, and generate" +
                " a DataStream consisting of all Spatial objects that have centroids (signals) within the range of POIs" +
                " (in different encounter modes). Polygon sizes should be considerably small, i.e. few hundred meters at most",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(INPUT_POINTS, "Spatial objects treated as signals",
                                StreamType.SPATIAL
                        )
                        .mandatoryInput(INPUT_POIS, "Source POI DataStream with vicinity radius property set",
                                StreamType.SPATIAL
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(ENCOUNTER_MODE, "How to treat signal a target one in regard of multiple POIs in the vicinity",
                                EncounterMode.class, EncounterMode.COPY, "By default, create a distinct copy of a signal for each POI" +
                                        " it encounters in the proximity radius")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .mandatoryOutput(OUTPUT_TARGET, "Output Point DataStream with target signals",
                                StreamType.SPATIAL, Origin.AUGMENTED, Arrays.asList(INPUT_POINTS, INPUT_POIS)
                        )
                        .generated(OUTPUT_TARGET, GEN_DISTANCE, "Distance from POI for " + ENCOUNTER_MODE + "=" + EncounterMode.COPY.name())
                        .optionalOutput(OUTPUT_EVICTED, "Optional output Point DataStream with evicted signals",
                                StreamType.SPATIAL, Origin.FILTERED, Collections.singletonList(INPUT_POINTS)
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

        DataStream inputSignals = inputStreams.get(INPUT_POINTS);
        DataStream inputPois = inputStreams.get(INPUT_POIS);

        // Get POIs radii
        JavaRDD<Tuple2<Double, PointEx>> poiRadii = inputPois.rdd
                .mapPartitions(it -> {
                    List<Tuple2<Double, PointEx>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        SpatialRecord<?> o = (SpatialRecord<?>) it.next()._2;

                        PointEx c = (PointEx) o.getCentroid();
                        c.put(o.asIs());
                        double radius = c.getRadius();
                        result.add(new Tuple2<>(radius, c));
                    }

                    return result.iterator();
                });

        final double maxRadius = poiRadii
                .map(t -> t._1)
                .max(Comparator.naturalOrder());

        final SpatialUtils spatialUtils = new SpatialUtils(maxRadius);

        // hash -> radius, poi
        JavaPairRDD<Long, Tuple2<Double, PointEx>> hashedPois = poiRadii
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Tuple2<Double, PointEx>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Double, PointEx> o = it.next();

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(o._2.getY(), o._2.getX()),
                                new Tuple2<>(o._1, o._2))
                        );
                    }

                    return result.iterator();
                });
        final long poiCount = hashedPois.count();

        Map<Long, Iterable<Tuple2<Double, PointEx>>> hashedPoisMap = hashedPois
                .groupByKey()
                .collectAsMap();

        JavaPairRDD<Object, Record<?>> signalsInput = inputSignals.rdd;

        // Broadcast hashed POIs
        Broadcast<HashMap<Long, Iterable<Tuple2<Double, PointEx>>>> broadcastHashedPois = JavaSparkContext.fromSparkContext(signalsInput.context())
                .broadcast(new HashMap<>(hashedPoisMap));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Object, Tuple2<Boolean, Record<?>>> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Tuple2<Double, PointEx>>> pois = broadcastHashedPois.getValue();

                    List<Tuple2<Object, Tuple2<Boolean, Record<?>>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> signal = it.next();
                        boolean target = false;

                        Point centroid = ((SpatialRecord<?>) signal._2).getCentroid();
                        double signalLat = centroid.getY();
                        double signalLon = centroid.getX();
                        List<Long> neighood = spatialUtils.getNeighbours(signalLat, signalLon);
                        long near = 0;

                        once:
                        for (Long hash : neighood) {
                            if (pois.containsKey(hash)) {
                                for (Tuple2<Double, PointEx> poi : pois.get(hash)) {
                                    double distance = Geodesic.WGS84.Inverse(signalLat, signalLon, poi._2.getY(), poi._2.getX(), GeodesicMask.DISTANCE).s12;

                                    //check if poi falls into radius
                                    switch (_once) {
                                        case ONCE: {
                                            if (distance <= poi._1) {
                                                result.add(new Tuple2<>(signal._1, new Tuple2<>(true, signal._2)));
                                                target = true;
                                                break once;
                                            }
                                            break;
                                        }
                                        case COPY: {
                                            if (distance <= poi._1) {
                                                SpatialRecord<?> point = (SpatialRecord<?>) signal._2.clone();
                                                point.put(poi._2.asIs());
                                                point.put(signal._2.asIs());
                                                point.put(GEN_DISTANCE, distance);
                                                result.add(new Tuple2<>(signal._1, new Tuple2<>(true, point)));
                                                target = true;
                                            }
                                            break;
                                        }
                                        case ALL: {
                                            if (distance > poi._1) {
                                                break once;
                                            } else {
                                                target = true;
                                                near++;
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (_once == EncounterMode.ALL) {
                            if (near == poiCount) {
                                result.add(new Tuple2<>(signal._1, new Tuple2<>(true, signal._2)));
                            } else {
                                target = false;
                            }
                        }
                        if (!target) {
                            result.add(new Tuple2<>(signal._1, new Tuple2<>(false, signal._2)));
                        }
                    }

                    return result.iterator();
                });

        Map<String, DataStream> ret = new HashMap<>();
        List<String> outputColumns = new ArrayList<>(inputSignals.accessor.attributes(OBJLVL_POINT));
        if (once == EncounterMode.COPY) {
            outputColumns.addAll(inputPois.accessor.attributes(OBJLVL_POINT));
            outputColumns.add(GEN_DISTANCE);
        }
        ret.put(outputStreams.get(OUTPUT_TARGET), new DataStream(StreamType.Point, signals
                .filter(t -> t._2._1)
                .mapToPair(t -> new Tuple2<>(t._1, t._2._2)), Collections.singletonMap(OBJLVL_POINT, outputColumns)));

        String outputEvictedName = outputStreams.get(OUTPUT_EVICTED);
        if (outputEvictedName != null) {
            ret.put(outputEvictedName, new DataStream(StreamType.Point, signals
                    .filter(t -> !t._2._1)
                    .mapToPair(t -> new Tuple2<>(t._1, t._2._2)), Collections.singletonMap(OBJLVL_POINT, inputSignals.accessor.attributes(OBJLVL_POINT))));
        }

        return Collections.unmodifiableMap(ret);
    }

    private enum EncounterMode implements DefinitionEnum {
        ONCE("This flag suppresses creation of copies of a signal for each proximal POI." +
                " Properties of the source signal will be unchanged"),
        COPY("For this flag, a distinct copy of source signal will be created for each proximal POI," +
                " and their properties will be augmented with properties of that POI"),
        ALL("This flag emits only signals that in the intersection of all input POI vicinities with unchanged properties." +
                " If POI vicinity radii don't intersect, no signals will be emitted");

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
