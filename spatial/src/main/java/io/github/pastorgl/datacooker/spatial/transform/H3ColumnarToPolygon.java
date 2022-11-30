/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class H3ColumnarToPolygon implements Transform {
    static final String HASH_COLUMN = "hash_column";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3ColumnarToPolygon", StreamType.Columnar, StreamType.Polygon,
                "Take a Columnar DataStream with H3 hashes and produce a Polygon DataStream",

                new DefinitionMetaBuilder()
                        .def(HASH_COLUMN, "H3 hash column")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_POLYGON);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_VALUE);
            }

            final String hashColumn = params.get(HASH_COLUMN);
            if (hashColumn == null) {
                throw new InvalidConfigurationException("Parameter hash.column is required to produce Polygons from Columnar H3 DataStream, " +
                        "but it wasn't specified");
            }

            List<String> _outputColumns = valueColumns;
            final GeometryFactory geometryFactory = new GeometryFactory();

            return new DataStream(StreamType.Polygon, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<Polygon> ret = new ArrayList<>();

                        H3Core h3 = H3Core.newInstance();

                        while (it.hasNext()) {
                            Columnar s = it.next();

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, s.asIs(col));
                            }

                            long hash = Long.parseUnsignedLong(s.asString(hashColumn), 16);
                            List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                            geo.add(geo.get(0));

                            List<Coordinate> cl = new ArrayList<>();
                            geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                            Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                            polygon.setUserData(props);

                            ret.add(polygon);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
