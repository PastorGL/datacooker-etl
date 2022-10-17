/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.config.Constants;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class TextToColumnarTransform implements Transform {
    static final String DELIMITER = "delimiter";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("textToColumnar", StreamType.PlainText, StreamType.Columnar,
                "This converts PlainText delimiter-separated DataStream into Columnar",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiting character", "\t", "By default, a tab character is used")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get(OBJLVL_VALUE);

            final char _inputDelimiter = ((String) params.get(DELIMITER)).charAt(0);

            return new DataStream(StreamType.Columnar, ((JavaRDD<Object>) ds.get())
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            String[] line = parser.parseLine(String.valueOf(it.next()));

                            Columnar rec = new Columnar(_outputColumns);
                            for (int i = 0, outputColumnsSize = _outputColumns.size(); i < outputColumnsSize; i++) {
                                String col = _outputColumns.get(i);
                                if (!col.equals(Constants.UNDERSCORE)) {
                                    rec.put(col, line[i]);
                                }
                            }

                            ret.add(rec);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
