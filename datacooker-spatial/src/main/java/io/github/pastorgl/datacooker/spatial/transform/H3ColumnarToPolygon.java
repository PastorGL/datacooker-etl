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
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import io.github.pastorgl.datacooker.data.spatial.SpatialUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class H3ColumnarToPolygon extends Transformer {
    static final String HASH_COLUMN = "hash_column";
    static final String VERB = "h3ColumnarToPolygon";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take a Columnar DataStream with H3 hashes and produce a Polygon DataStream")
                .transform().objLvls(POLYGON).operation()
                .input(StreamType.COLUMNAR, "Input H3 Columnar DS")
                .output(StreamType.POLYGON, "Output Polygon DS")
                .def(HASH_COLUMN, "H3 hash column")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(POLYGON) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(VALUE);
            }

            final String hashColumn = params.get(HASH_COLUMN);

            final List<String> _outputColumns = valueColumns;

            final GeometryFactory geometryFactory = new GeometryFactory();

            return new DataStreamBuilder(outputName, Collections.singletonMap(POLYGON, _outputColumns))
                    .transformed(VERB, StreamType.Polygon, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, t._2.asIs(col));
                            }

                            long hash = Long.parseUnsignedLong(t._2.asString(hashColumn), 16);
                            List<LatLng> geo = SpatialUtils.H3.cellToBoundary(hash);
                            geo.add(geo.get(0));

                            List<Coordinate> cl = new ArrayList<>();
                            geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                            PolygonEx polygon = new PolygonEx(geometryFactory.createPolygon(cl.toArray(new Coordinate[0])));
                            polygon.put(props);

                            ret.add(new Tuple2<>(t._1, polygon));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
