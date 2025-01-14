/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class ColumnarToStructuredTransform extends Transform {
    static final String TEMPLATE = "template";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToStructured", StreamType.Columnar, StreamType.Structured,
                "Transform Columnar records to Structured objects",

                new DefinitionMetaBuilder()
                        .def(TEMPLATE, "Structured object template in JSON format. Refer to source columns with $column_name$ notation")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final String template = params.get(TEMPLATE);

            List<String> valueColumns = newColumns.get(VALUE);
            if (valueColumns == null) {
                valueColumns = ds.attributes(VALUE);
            }

            final List<String> _outputColumns = valueColumns;
            return new DataStreamBuilder(ds.name, Collections.singletonMap(VALUE, _outputColumns))
                    .transformed(meta.verb, StreamType.Structured, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        ObjectMapper om = new ObjectMapper();
                        om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> line = it.next();

                            String _template = template;
                            for (String columnName : _outputColumns) {
                                _template = _template.replaceAll("\\$" + columnName + "\\$", line._2.asString(columnName));
                            }
                            ret.add(new Tuple2<>(line._1, new Structured(om.readValue(_template, Object.class))));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
