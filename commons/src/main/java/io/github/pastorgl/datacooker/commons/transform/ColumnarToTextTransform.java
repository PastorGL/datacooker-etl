/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVWriter;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ColumnarToTextTransform implements Transform {
    static final String DELIMITER = "delimiter";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToText", StreamType.Columnar, StreamType.PlainText,
                "Transform Columnar DataStream to delimited text",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                        .build(),
                null
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

            List<String> _outputColumns = valueColumns;
            int len = _outputColumns.size();

            return new DataStream(StreamType.PlainText, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar line = it.next();

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                columns[i] = String.valueOf(line.asIs(_outputColumns.get(i)));
                            }
                            writer.writeNext(columns, false);
                            writer.close();

                            ret.add(new Text(buffer.toString()));
                        }

                        return ret.iterator();
                    }), null);
        };
    }
}
