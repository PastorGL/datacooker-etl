/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVWriter;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ColumnarToTextTransform extends Transform {
    static final String GEN_KEY = "_key";

    static final String DELIMITER = "delimiter";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToText", StreamType.Columnar, StreamType.PlainText,
                "Transform Columnar DataStream to delimited text",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_KEY, "Key of the record")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final char delimiter = ((String) params.get(DELIMITER)).charAt(0);

            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_VALUE);
            }

            final List<String> _outputColumns = valueColumns;
            final int len = _outputColumns.size();

            return new DataStream(StreamType.PlainText, ds.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> o = it.next();

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                String key = _outputColumns.get(i);
                                columns[i] = key.equalsIgnoreCase(GEN_KEY) ? String.valueOf(o._1) : String.valueOf(o._2.asIs(key));
                            }
                            writer.writeNext(columns, false);
                            writer.close();

                            ret.add(new Tuple2<>(o._1, new PlainText(buffer.toString())));
                        }

                        return ret.iterator();
                    }, true), null);
        };
    }
}
