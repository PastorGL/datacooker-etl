/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.PlainText;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import io.github.pastorgl.datacooker.spatial.functions.PolygonConverter;
import org.wololo.geojson.Feature;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PolygonToGeoJsonTransform extends Transformer {

    static final String VERB = "polygonToGeoJson";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take a Polygon DataStream and produce a Plain Text DataStream with GeoJSON fragments")
                .transform(true).objLvls(VALUE).operation()
                .input(StreamType.POLYGON, "Input Polygon DS")
                .output(StreamType.PLAIN_TEXT, "Output GeoJson DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(VALUE) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(POLYGON);
            }

            final List<String> _outputColumns = valueColumns;

            return new DataStreamBuilder(outputName, null)
                    .transformed(VERB, StreamType.PlainText, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            PolygonEx poly = (PolygonEx) t._2;
                            Map<String, Object> featureProps = new HashMap<>();
                            for (String column : _outputColumns) {
                                featureProps.put(column, poly.asIs(column));
                            }

                            ret.add(new Tuple2<>(t._1, new PlainText(new Feature(PolygonConverter.convert(poly), featureProps).toString())));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
