/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class TextToPairTransform implements Transform {
    static final String DELIMITER = "delimiter";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("textToPair", StreamType.PlainText, StreamType.KeyValue,
                "Transform delimited text DataStream to KeyValue",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get(OBJLVL_VALUE);

            final char _inputDelimiter = ((String) params.get(DELIMITER)).charAt(0);

            return new DataStream(StreamType.KeyValue, ((JavaPairRDD<String, Object>) ds.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            Tuple2<String, Object> line = it.next();
                            String[] l = parser.parseLine(String.valueOf(line._2));

                            Columnar rec = new Columnar(_outputColumns);
                            for (int i = 0, outputColumnsSize = _outputColumns.size(); i < outputColumnsSize; i++) {
                                String col = _outputColumns.get(i);
                                if (!col.equals(Constants.UNDERSCORE)) {
                                    rec.put(col, l[i]);
                                }
                            }

                            ret.add(new Tuple2<>(String.valueOf(line._1), rec));
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
