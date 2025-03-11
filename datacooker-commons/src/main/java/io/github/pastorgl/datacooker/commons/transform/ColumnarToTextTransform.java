/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVWriter;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class ColumnarToTextTransform extends Transformer {
    static final String GEN_KEY = "_key";

    static final String DELIMITER = "delimiter";
    static final String VERB = "columnarToText";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform Columnar DataStream to delimited text")
                .transform(true).objLvls(VALUE).operation()
                .input(StreamType.COLUMNAR, "Input Columnar DS")
                .output(StreamType.PLAIN_TEXT, "Output delimited text DS")
                .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                .generated(GEN_KEY, "Key of the record")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final String delimiter = params.get(DELIMITER);
            final char _delimiter = delimiter.charAt(0);

            List<String> valueColumns = (newColumns != null) ? newColumns.get(VALUE) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(VALUE);
            }

            final List<String> _outputColumns = valueColumns;
            final int len = _outputColumns.size();

            return new DataStreamBuilder(outputName, null)
                    .transformed(VERB, StreamType.PlainText, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> o = it.next();

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, _delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                String key = _outputColumns.get(i);
                                if (key.equals(GEN_KEY)) {
                                    columns[i] = o._1.getClass().isArray() ? Arrays.stream((Object[]) o._1).map(String::valueOf).collect(Collectors.joining(delimiter)) : String.valueOf(o._1);
                                } else {
                                    columns[i] = String.valueOf(o._2.asIs(key));
                                }
                            }
                            writer.writeNext(columns, false);
                            writer.close();

                            ret.add(new Tuple2<>(o._1, new PlainText(buffer.toString())));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
