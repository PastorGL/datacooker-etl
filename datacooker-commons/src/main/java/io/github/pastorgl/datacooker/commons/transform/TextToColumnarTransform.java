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
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class TextToColumnarTransform extends Transformer {
    static final String DELIMITER = "delimiter";
    static final String VERB = "textToColumnar";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform delimited text DataStream to Columnar. Does not preserve partitioning. To skip a column," +
                        " reference it as _ (underscore)")
                .transform().objLvls(true, VALUE)
                .input(StreamType.PLAIN_TEXT, "Input delimited text DS")
                .output(StreamType.COLUMNAR, "Output Columnar DS")
                .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final char _inputDelimiter = ((String) params.get(DELIMITER)).charAt(0);

            final String[] _outputColumns = newColumns.get(VALUE).toArray(new String[0]);
            final List<String> outputColumns = Arrays.stream(_outputColumns)
                    .filter(col -> !Constants.UNDERSCORE.equals(col))
                    .collect(Collectors.toList());

            return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, outputColumns))
                    .transformed(VERB, StreamType.Columnar, ds)
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
