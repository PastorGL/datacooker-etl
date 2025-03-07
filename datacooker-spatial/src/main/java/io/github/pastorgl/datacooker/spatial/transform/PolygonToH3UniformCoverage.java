/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import com.uber.h3core.util.LatLng;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.locationtech.jts.geom.Coordinate;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PolygonToH3UniformCoverage extends Transform {
    static final String HASH_LEVEL = "hash_level";
    static final String GEN_HASH = "_hash";
    static final String VERB = "h3UniformCoverage";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Create a uniform (non-compact) H3 coverage" +
                        " from the Polygon DataStream. Does not preserve partitioning")
                .transform().objLvls(VALUE).operation()
                .input(StreamType.POLYGON, "Input Polygon DS")
                .output(StreamType.COLUMNAR, "Output Columnar DS")
                .def(HASH_LEVEL, "Level of the hash",
                        Integer.class, 9, "Default H3 hash level")
                .generated(GEN_HASH, "Polygon H3 hash")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(VALUE) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(POLYGON);
            }

            final List<String> _outputColumns = valueColumns;

            final int level = params.get(HASH_LEVEL);

            return new DataStreamBuilder(ds.name, Collections.singletonMap(VALUE, _outputColumns))
                    .transformed(VERB, StreamType.Columnar, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        Set<DataRecord<?>> ret = new HashSet<>();

                        Random random = new Random();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

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

                            Set<Long> polyfill = new HashSet<>(SpatialUtils.H3.polygonToCells(gco, gci, level));

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

                        return ret.stream().map(r -> new Tuple2<Object, DataRecord<?>>(random.nextLong(), r)).iterator();
                    }));
        };
    }
}
