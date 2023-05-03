/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class CountUniquesOperation extends Operation {
    static final String COUNT_COLUMNS = "count_columns";

    protected String[] countColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("countUniques", "Statistical indicator for counting unique values in each of selected" +
                " attributes of DataStream per each unique key. Names of referenced attributes have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("KeyValue DataStream to count uniques per key",
                                StreamType.ATTRIBUTED
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(COUNT_COLUMNS, "Attributes to count unique values under same keys", String[].class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Columnar OUTPUT DataStream with unique values counts",
                                new StreamType[]{StreamType.Columnar}, Origin.GENERATED, null
                        )
                        .generated("*", "Generated column names are same as source names enumerated in '" + COUNT_COLUMNS + "'")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        countColumns = params.get(COUNT_COLUMNS);
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final List<String> outputColumns = Arrays.asList(countColumns);
        final int l = countColumns.length;

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            JavaPairRDD<Object, Record<?>> out = inputStreams.getValue(i).rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object[]>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> next = it.next();

                            Object[] value = new Object[l];
                            for (int j = 0; j < l; j++) {
                                value[j] = next._2.asIs(outputColumns.get(j));
                            }

                            ret.add(new Tuple2<>(next._1, value));
                        }

                        return ret.iterator();
                    })
                    .combineByKey(
                            t -> {
                                HashSet<Object>[] s = new HashSet[l];
                                for (int j = 0; j < l; j++) {
                                    s[j] = new HashSet<>();
                                    s[j].add(t[j]);
                                }
                                return s;
                            },
                            (c, t) -> {
                                for (int j = 0; j < l; j++) {
                                    c[j].add(t[j]);
                                }
                                return c;
                            },
                            (c1, c2) -> {
                                for (int j = 0; j < l; j++) {
                                    c1[j].addAll(c2[j]);
                                }
                                return c1;
                            }
                    )
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, HashSet<Object>[]> next = it.next();

                            Object[] r = new Object[l];
                            for (int j = 0; j < l; j++) {
                                r[j] = next._2[j].size();
                            }

                            ret.add(new Tuple2<>(next._1, new Columnar(outputColumns, r)));
                        }

                        return ret.iterator();
                    });

            output.put(outputStreams.get(i), new DataStream(StreamType.Columnar, out, Collections.singletonMap(OBJLVL_VALUE, outputColumns)));
        }

        return output;
    }
}
