/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PolygonToH3UniformCoverage implements Transform {
    static final String HASH_LEVEL = "hash.level";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3UniformCoverage", StreamType.Polygon, StreamType.Columnar,
                "Create a uniform (non-compact) H3 coverage" +
                        " from the Polygon or Point DataStream. Can pass any properties from the source geometries to the resulting" +
                        " Columnar attributes, for each hash per each geometry",

                new DefinitionMetaBuilder()
                        .def(HASH_LEVEL, "Level of the hash",
                                Integer.class, 9, "Default H3 hash level")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_POLYGON);
            }

            final List<String> _outputColumns = valueColumns;

            final int level = params.get(HASH_LEVEL);

            return new DataStream(StreamType.Columnar, ((JavaRDD<Polygon>) ds.get()).mapPartitions(it -> {
                Set<Columnar> ret = new HashSet<>();

                H3Core h3 = H3Core.newInstance();

                while (it.hasNext()) {
                    Polygon p = it.next();

                    Map<String, Object> props = (Map<String, Object>) p.getUserData();

                    List<GeoCoord> gco = new ArrayList<>();
                    for (Coordinate c : p.getExteriorRing().getCoordinates()) {
                        gco.add(new GeoCoord(c.y, c.x));
                    }

                    List<List<GeoCoord>> gci = new ArrayList<>();
                    for (int i = p.getNumInteriorRing(); i > 0; ) {
                        List<GeoCoord> gcii = new ArrayList<>();
                        for (Coordinate c : p.getInteriorRingN(--i).getCoordinates()) {
                            gcii.add(new GeoCoord(c.y, c.x));
                        }
                        gci.add(gcii);
                    }

                    Set<Long> polyfill = new HashSet<>(h3.polyfill(gco, gci, level));

                    for (Long hash : polyfill) {
                        Columnar rec = new Columnar(_outputColumns);

                        for (String column : _outputColumns) {
                            if ("_hash".equals(column)) {
                                rec.put(column, Long.toHexString(hash));
                            } else {
                                rec.put(column, props.get(column));
                            }
                        }

                        ret.add(rec);
                    }
                }

                return ret.iterator();
            }), newColumns);
        };
    }
}
