/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.metadata.StreamOrigin;
import io.github.pastorgl.datacooker.scripting.Operation;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;

@SuppressWarnings("unused")
public class PolygonStatsOperation extends Operation {
    private static final String GEN_AREA = "_area";
    private static final String GEN_PERIMETER = "_perimeter";
    private static final String GEN_VERTICES = "_vertices";
    private static final String GEN_HOLES = "_holes";

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonStats", "Take a Polygon DataStream and augment its properties with simple statistics",

                new PositionalStreamsMetaBuilder()
                        .input("Source Polygon DataStream",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .output("Output Polygon DataStream",
                                new StreamType[]{StreamType.Polygon}, StreamOrigin.AUGMENTED, null
                        )
                        .generated(GEN_HOLES, "Number of Polygon holes")
                        .generated(GEN_PERIMETER, "Polygon perimeter in meters")
                        .generated(GEN_VERTICES, "Number of Polygon vertices")
                        .generated(GEN_AREA, "Polygon area in square meters")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, Record<?>> out = input.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> result = new ArrayList<>();

                        PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> next = it.next();

                            pArea.Clear();
                            PolygonEx p = (PolygonEx) next._2;
                            for (Coordinate c : p.getExteriorRing().getCoordinates()) {
                                pArea.AddPoint(c.y, c.x);
                            }

                            PolygonResult pRes = pArea.Compute();

                            int numHoles = p.getNumInteriorRing();
                            p.put(GEN_HOLES, numHoles);
                            p.put(GEN_PERIMETER, pRes.perimeter);
                            p.put(GEN_VERTICES, pRes.num);

                            double area = Math.abs(pRes.area);
                            for (int hole = numHoles; hole > 0; hole--) {
                                LineString lr = p.getInteriorRingN(hole - 1);

                                pArea.Clear();
                                for (Coordinate c : lr.getCoordinates()) {
                                    pArea.AddPoint(c.y, c.x);
                                }

                                area -= Math.abs(pArea.Compute().area);
                            }
                            p.put(GEN_AREA, area);

                            result.add(new Tuple2<>(next._1, p));
                        }

                        return result.iterator();
                    });

            List<String> polygonProps = input.accessor.attributes(OBJLVL_POLYGON);
            List<String> outputColumns = (polygonProps == null) ? new ArrayList<>() : new ArrayList<>(polygonProps);
            outputColumns.add(GEN_HOLES);
            outputColumns.add(GEN_PERIMETER);
            outputColumns.add(GEN_VERTICES);
            outputColumns.add(GEN_AREA);

            output.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), StreamType.Polygon, Collections.singletonMap(OBJLVL_POLYGON, outputColumns))
                    .augmented(meta.verb, input)
                    .build(out)
            );
        }

        return output;
    }
}
