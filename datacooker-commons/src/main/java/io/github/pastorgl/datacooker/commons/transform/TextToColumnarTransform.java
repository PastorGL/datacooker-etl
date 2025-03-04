/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class TextToColumnarTransform extends Transform {
    static final String DELIMITER = "delimiter";

    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("textToColumnar",
                "Transform delimited text DataStream to Columnar. Does not preserve partitioning. To skip a column," +
                        " reference it as _ (underscore)")
                .transform(StreamType.PlainText, StreamType.Columnar).objLvls(true, VALUE)
                .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final char _inputDelimiter = ((String) params.get(DELIMITER)).charAt(0);

            final String[] _outputColumns = newColumns.get(VALUE).toArray(new String[0]);
            final List<String> outputColumns = Arrays.stream(_outputColumns)
                    .filter(col -> !Constants.UNDERSCORE.equals(col))
                    .collect(Collectors.toList());

            return new DataStreamBuilder(ds.name, Collections.singletonMap(VALUE, outputColumns))
                    .transformed(meta.verb, StreamType.Columnar, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            String[] line = parser.parseLine(String.valueOf(t._2));
                            Columnar rec = new Columnar(outputColumns);
                            for (int i = 0; i < _outputColumns.length; i++) {
                                if (!Constants.UNDERSCORE.equals(_outputColumns[i])) {
                                    rec.put(_outputColumns[i], line[i]);
                                }
                            }

                            ret.add(new Tuple2<>(t._1, rec));
                        }

                        return ret.iterator();
                    }));
        };
    }
}
