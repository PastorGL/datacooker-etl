/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.locationtech.jts.geom.Coordinate;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PolygonToH3UniformCoverage extends Transform {
    static final String HASH_LEVEL = "hash_level";
    static final String GEN_HASH = "_hash";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3UniformCoverage", StreamType.Polygon, StreamType.Columnar,
                "Create a uniform (non-compact) H3 coverage" +
                        " from the Polygon DataStream. Does not preserve partitioning",

                new DefinitionMetaBuilder()
                        .def(HASH_LEVEL, "Level of the hash",
                                Integer.class, 9, "Default H3 hash level")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_HASH, "Polygon H3 hash")
                        .build(),
                true
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

            return new DataStream(StreamType.Columnar, ds.rdd
                    .mapPartitionsToPair(it -> {
                        Set<Record<?>> ret = new HashSet<>();

                        H3Core h3 = H3Core.newInstance();
                        Random random = new Random();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> t = it.next();

                            PolygonEx p = (PolygonEx) t._2;
                            Map<String, Object> props = p.asIs();

                            List<LatLng> gco = new ArrayList<>();
                            for (Coordinate c : p.getExteriorRing().getCoordinates()) {
                                gco.add(new LatLng(c.y, c.x));
                            }

                            List<List<LatLng>> gci = new ArrayList<>();
                            for (int i = p.getNumInteriorRing(); i > 0; ) {
                                List<LatLng> gcii = new ArrayList<>();
                                for (Coordinate c : p.getInteriorRingN(--i).getCoordinates()) {
                                    gcii.add(new LatLng(c.y, c.x));
                                }
                                gci.add(gcii);
                            }

                            Set<Long> polyfill = new HashSet<>(h3.polygonToCells(gco, gci, level));

                            for (Long hash : polyfill) {
                                Columnar rec = new Columnar(_outputColumns);

                                for (String column : _outputColumns) {
                                    if (GEN_HASH.equals(column)) {
                                        rec.put(column, Long.toHexString(hash));
                                    } else {
                                        rec.put(column, props.get(column));
                                    }
                                }

                                ret.add(rec);
                            }
                        }

                        return ret.stream().map(r -> new Tuple2<Object, Record<?>>(random.nextLong(), r)).iterator();
                    }, false), Collections.singletonMap(OBJLVL_VALUE, _outputColumns));
        };
    }
}
