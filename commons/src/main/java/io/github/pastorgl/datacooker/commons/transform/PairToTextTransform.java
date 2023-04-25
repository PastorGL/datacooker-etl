/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.opencsv.CSVWriter;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PairToTextTransform implements Transform {
    static final String DELIMITER = "delimiter";
    static final String GEN_KEY = "_key";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("pairToText", StreamType.KeyValue, StreamType.PlainText,
                "Transform KeyValue DataStream to delimited text",

                new DefinitionMetaBuilder()
                        .def(DELIMITER, "Column delimiter", "\t", "By default, tab character")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_KEY, "Key of the Pair")
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

            List<String> _outputColumns = valueColumns;
            int len = _outputColumns.size();

            return new DataStream(StreamType.PlainText, ((JavaPairRDD<String, Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<PlainText> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, Columnar> o = it.next();
                            Columnar line = o._2;

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                String key = _outputColumns.get(i);
                                columns[i] = key.equalsIgnoreCase(GEN_KEY) ? String.valueOf(o._1) : String.valueOf(line.asIs(key));
                            }
                            writer.writeNext(columns, false);
                            writer.close();

                            ret.add(new PlainText(buffer.toString()));
                        }

                        return ret.iterator();
                    }), null);
        };
    }
}
