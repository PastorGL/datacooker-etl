/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.TransformerOperation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class CountUniquesOperation extends TransformerOperation {
    static final String COUNT_ATTRS = "count_attrs";
    static final String VERB = "countUniques";

    protected Object[] countAttrs;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Statistical indicator for counting unique values in each of selected" +
                " attributes of DataStream per each unique key. Names of referenced attributes have to be same in each INPUT DataStream")
                .operation()
                .input(StreamType.ATTRIBUTED, "KeyValue DataStream to count uniques per key")
                .def(COUNT_ATTRS, "Attributes to count unique values under same keys", Object[].class)
                .output(StreamType.COLUMNAR, "Columnar OUTPUT DataStream with unique values counts",
                        StreamOrigin.GENERATED, null)
                .generated("*", "Generated column names are same as source names enumerated in '" + COUNT_ATTRS + "'")
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        countAttrs = params.get(COUNT_ATTRS);
    }

    @Override
    public StreamTransformer transformer() {
        final List<String> outputColumns = Arrays.stream(countAttrs).map(String::valueOf).collect(Collectors.toList());
        final int l = countAttrs.length;

        return (input, name) -> {
            JavaPairRDD<Object, DataRecord<?>> out = input.rdd()
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object[]>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> next = it.next();

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
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

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

            return new DataStreamBuilder(name, Collections.singletonMap(VALUE, outputColumns))
                    .generated(VERB, StreamType.Columnar, input)
                    .build(out);
        };
    }
}
