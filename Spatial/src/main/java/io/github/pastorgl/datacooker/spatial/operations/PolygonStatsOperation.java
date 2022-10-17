/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.scripting.Operation;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POLYGON;

@SuppressWarnings("unused")
public class PolygonStatsOperation extends Operation {
    private static final String GEN_AREA = "_area";
    private static final String GEN_PERIMETER = "_perimeter";
    private static final String GEN_VERTICES = "_vertices";
    private static final String GEN_HOLES = "_holes";

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonStats", "Take a Polygon DataStream and augment its properties with statistics",

                new PositionalStreamsMetaBuilder()
                        .input("Polygon RDD to count the stats",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .output("Polygon RDD with stat parameters calculated",
                                new StreamType[]{StreamType.Polygon}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_HOLES, "Number of Polygon holes")
                        .generated(GEN_PERIMETER, "Polygon perimeter in meters")
                        .generated(GEN_VERTICES, "Number of Polygon vertices")
                        .generated(GEN_AREA, "Polygon area in square meters")
                        .build()
        );
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        DataStream input = inputStreams.getValue(0);
        JavaRDD<PolygonEx> output = ((JavaRDD<PolygonEx>) input.get())
                .mapPartitions(it -> {
                    List<PolygonEx> result = new ArrayList<>();

                    PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

                    while (it.hasNext()) {
                        PolygonEx next = it.next();

                        pArea.Clear();
                        for (Coordinate c : next.getExteriorRing().getCoordinates()) {
                            pArea.AddPoint(c.y, c.x);
                        }

                        PolygonResult pRes = pArea.Compute();

                        int numHoles = next.getNumInteriorRing();
                        next.put(GEN_HOLES, numHoles);
                        next.put(GEN_PERIMETER, pRes.perimeter);
                        next.put(GEN_VERTICES, pRes.num);

                        double area = Math.abs(pRes.area);
                        for (int hole = numHoles; hole > 0; hole--) {
                            LineString lr = next.getInteriorRingN(hole - 1);

                            pArea.Clear();
                            for (Coordinate c : lr.getCoordinates()) {
                                pArea.AddPoint(c.y, c.x);
                            }

                            area -= Math.abs(pArea.Compute().area);
                        }
                        next.put(GEN_AREA, area);

                        result.add(next);
                    }

                    return result.iterator();
                });

        List<String> polygonProps = input.accessor.attributes(OBJLVL_POLYGON);
        List<String> outputColumns = (polygonProps == null) ? new ArrayList<>() : new ArrayList<>(polygonProps);
        outputColumns.add(GEN_HOLES);
        outputColumns.add(GEN_PERIMETER);
        outputColumns.add(GEN_VERTICES);
        outputColumns.add(GEN_AREA);
        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Polygon, output, Collections.singletonMap(OBJLVL_POLYGON, outputColumns)));
    }
}
