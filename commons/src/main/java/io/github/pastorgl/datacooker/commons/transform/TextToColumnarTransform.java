/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class TextToColumnarTransform extends Transform {
    static final String DELIMITER = "delimiter";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("textToColumnar", StreamType.PlainText, StreamType.Columnar,
                "Transform delimited text DataStream to Columnar. Does not preserve partitioning",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final char _inputDelimiter = ((String) params.get(DELIMITER)).charAt(0);

            final List<String> _outputColumns = newColumns.get(OBJLVL_VALUE);

            return new DataStream(StreamType.Columnar, ds.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> t = it.next();

                            String[] line = parser.parseLine(String.valueOf(t._2));
                            Columnar rec = new Columnar(_outputColumns);
                            for (int i = 0, outputColumnsSize = _outputColumns.size(); i < outputColumnsSize; i++) {
                                String col = _outputColumns.get(i);
                                if (!col.equals(Constants.UNDERSCORE)) {
                                    rec.put(col, line[i]);
                                }
                            }

                            ret.add(new Tuple2<>(t._1, rec));
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
