/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class CountUniquesOperation extends Operation {
    static final String COUNT_COLUMNS = "count_columns";

    protected String[] countColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("countUniques", "Statistical indicator for counting unique values in each of selected" +
                " columns of KeyValue DataStream per each unique key",

                new PositionalStreamsMetaBuilder()
                        .input("KeyValue DataStream to count uniques per key",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(COUNT_COLUMNS, "Columns to count unique values under same keys", String[].class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("KeyValue output DataStream with unique values counts",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, null
                        )
                        .generated("*", "Generated column names are same as source names enumerated in '" + COUNT_COLUMNS + "'")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        countColumns = params.get(COUNT_COLUMNS);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final List<String> outputColumns = Arrays.asList(countColumns);
        final int l = countColumns.length;

        JavaPairRDD<String, Columnar> output = ((JavaPairRDD<String, Columnar>) inputStreams.getValue(0).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Object[]>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, Columnar> next = it.next();

                        Object[] value = new Object[l];
                        for (int i = 0; i < l; i++) {
                            value[i] = next._2.asIs(outputColumns.get(i));
                        }

                        ret.add(new Tuple2<>(next._1, value));
                    }

                    return ret.iterator();
                })
                .combineByKey(
                        t -> {
                            HashSet[] s = new HashSet[l];
                            for (int i = 0; i < l; i++) {
                                s[i] = new HashSet();
                                s[i].add(t[i]);
                            }
                            return s;
                        },
                        (c, t) -> {
                            for (int i = 0; i < l; i++) {
                                c[i].add(t[i]);
                            }
                            return c;
                        },
                        (c1, c2) -> {
                            for (int i = 0; i < l; i++) {
                                c1[i].addAll(c2[i]);
                            }
                            return c1;
                        }
                )
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, HashSet[]> next = it.next();

                        Object[] r = new Object[l];
                        for (int i = 0; i < l; i++) {
                            r[i] = next._2[i].size();
                        }

                        ret.add(new Tuple2<>(next._1, new Columnar(outputColumns, r)));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.KeyValue, output, Collections.singletonMap(OBJLVL_VALUE, outputColumns)));
    }
}
