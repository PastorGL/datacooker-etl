/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.proximity;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.DescribedEnum;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.FullOperation;
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

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;

@SuppressWarnings("unused")
public class ProximityOperation extends FullOperation {
    static final String INPUT_POINTS = "points";
    static final String INPUT_POIS = "pois";
    static final String OUTPUT_TARGET = "target";
    static final String OUTPUT_EVICTED = "evicted";

    static final String ENCOUNTER_MODE = "encounter_mode";

    static final String GEN_DISTANCE = "_distance";
    static final String VERB = "proximity";

    private EncounterMode once;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Take a Spatial DataStream and POI DataStream, and generate" +
                " a DataStream consisting of all Spatial objects that have centroids (signals) within the range of POIs" +
                " (in different encounter modes). Polygon sizes should be considerably small, i.e. few hundred meters at most")
                .operation()
                .input(INPUT_POINTS, StreamType.SPATIAL, "Spatial objects treated as signals")
                .input(INPUT_POIS, StreamType.SPATIAL, "Source POI DataStream with vicinity radius property set")
                .def(ENCOUNTER_MODE, "How to treat signal a target one in regard of multiple POIs in the vicinity",
                        EncounterMode.class, EncounterMode.COPY, "By default, create a distinct copy of a signal for each POI" +
                                " it encounters in the proximity radius")
                .output(OUTPUT_TARGET, StreamType.SPATIAL, "Output Point DataStream with target signals",
                        StreamOrigin.AUGMENTED, Arrays.asList(INPUT_POINTS, INPUT_POIS))
                .generated(OUTPUT_TARGET, GEN_DISTANCE, "Distance from POI for " + ENCOUNTER_MODE + "=" + EncounterMode.COPY.name())
                .optOutput(OUTPUT_EVICTED, StreamType.SPATIAL, "Optional output Point DataStream with evicted signals",
                        StreamOrigin.FILTERED, Collections.singletonList(INPUT_POINTS))
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        once = params.get(ENCOUNTER_MODE);
    }

    @Override
    public void execute() {
        EncounterMode _once = once;

        DataStream inputSignals = inputStreams.get(INPUT_POINTS);
        DataStream inputPois = inputStreams.get(INPUT_POIS);

        // Get POIs radii
        JavaRDD<Tuple2<Double, PointEx>> poiRadii = inputPois.rdd()
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

        JavaPairRDD<Object, DataRecord<?>> signalsInput = inputSignals.rdd();

        // Broadcast hashed POIs
        Broadcast<HashMap<Long, Iterable<Tuple2<Double, PointEx>>>> broadcastHashedPois = JavaSparkContext.fromSparkContext(signalsInput.context())
                .broadcast(new HashMap<>(hashedPoisMap));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Object, Tuple2<Boolean, DataRecord<?>>> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Tuple2<Double, PointEx>>> pois = broadcastHashedPois.getValue();

                    List<Tuple2<Object, Tuple2<Boolean, DataRecord<?>>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> signal = it.next();
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

        List<String> outputColumns = new ArrayList<>(inputSignals.attributes(POINT));
        if (once == EncounterMode.COPY) {
            outputColumns.addAll(inputPois.attributes(POINT));
            outputColumns.add(GEN_DISTANCE);
        }
        outputs.put(OUTPUT_TARGET, new DataStreamBuilder(outputStreams.get(OUTPUT_TARGET), Collections.singletonMap(POINT, outputColumns))
                .augmented(VERB, inputSignals, inputPois)
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
