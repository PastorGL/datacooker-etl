/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PercentRankIncOperation extends Transformer {
    private static final String PER_KEY = "per_key";
    static final String VALUE_ATTR = "value_attr";

    static final String GEN_VALUE = "_value";
    static final String GEN_RANK = "_rank";
    static final String VERB = "percentRankInc";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Statistical indicator for 'percentile rank inclusive'" +
                " function for a Double input value attribute. Output is fixed to value then rank attributes. Does not work" +
                " with datasets consisting of less than one element, and returns NaN for single-element dataset")
                .operation().transform()
                .input(StreamType.ATTRIBUTED, "INPUT with value attribute to calculate the rank")
                .def(PER_KEY, "If set, calculate rank per each key separately and put under that key",
                        Boolean.class, false, "By default, use entire DataStream as source. OUTPUT keys are counts")
                .def(VALUE_ATTR, "Attribute for counting rank values, must be of type Double")
                .output(StreamType.COLUMNAR, "OUTPUT with value ranks",
                        StreamOrigin.GENERATED, null)
                .generated(GEN_VALUE, "Ranked value")
                .generated(GEN_RANK, "Calculated rank")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (input, ignore, params) -> {
            final String _valueColumn = params.get(VALUE_ATTR);
            final boolean _perKey = params.get(PER_KEY);

            final List<String> outputColumns = Arrays.asList(GEN_VALUE, GEN_RANK);

            JavaPairRDD<Object, DataRecord<?>> output;
            if (!_perKey) {
                JavaPairRDD<Double, Long> valueCounts = input.rdd()
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Double, Long>> ret = new ArrayList<>();
                            while (it.hasNext()) {
                                DataRecord<?> row = it.next()._2;

                                Double value = row.asDouble(_valueColumn);
                                ret.add(new Tuple2<>(value, 1L));
                            }

                            return ret.iterator();
                        })
                        .reduceByKey(Long::sum)
                        .sortByKey();

                final double total = valueCounts.values().reduce(Long::sum) - 1L;

                Map<Integer, Long> partCounts = valueCounts
                        .mapPartitionsWithIndex((idx, it) -> {
                            long ret = 0L;

                            while (it.hasNext()) {
                                ret += it.next()._2;
                            }

                            return Collections.singletonMap(idx, ret).entrySet().iterator();
                        }, true)
                        .collect().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                Broadcast<HashMap<Integer, Long>> pc = JavaSparkContext.fromSparkContext(valueCounts.context()).broadcast(new HashMap<>(partCounts));
                output = valueCounts
                        .mapPartitionsWithIndex((idx, it) -> {
                            Map<Integer, Long> prevCounts = pc.getValue();

                            double global = prevCounts.entrySet().stream()
                                    .filter(e -> e.getKey() < idx)
                                    .map(Map.Entry::getValue)
                                    .reduce(0L, Long::sum);

                            List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                            while (it.hasNext()) {
                                Tuple2<Double, Long> value = it.next();

                                Columnar acc = new Columnar(outputColumns, new Object[]{value._1, global / total});
                                for (int j = 0; j < value._2; j++) {
                                    ret.add(new Tuple2<>(value._2, acc));
                                }

                                global += value._2;
                            }

                            return ret.iterator();
                        }, true)
                        .mapToPair(t -> t);
            } else {
                output = input.rdd()
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Object, Double>> ret = new ArrayList<>();
                            while (it.hasNext()) {
                                Tuple2<Object, DataRecord<?>> t = it.next();

                                Double value = t._2.asDouble(_valueColumn);

                                ret.add(new Tuple2<>(t._1, value));
                            }

                            return ret.iterator();
                        })
                        .aggregateByKey(
                                new ArrayList<Double>(),
                                (l, t) -> {
                                    l.add(t);
                                    Collections.sort(l);
                                    return l;
                                },
                                (l1, l2) -> {
                                    l1.addAll(l2);
                                    Collections.sort(l1);
                                    return l1;
                                }
                        )
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                            while (it.hasNext()) {
                                Tuple2<Object, ArrayList<Double>> next = it.next();

                                ArrayList<Double> value = next._2;

                                int size = value.size();
                                double total = size - 1;
                                int global = 0;
                                for (int j = 0; j < size; j++) {
                                    if ((j > 0) && (value.get(j - 1) < value.get(j))) {
                                        global = j;
                                    }

                                    ret.add(new Tuple2<>(next._1, new Columnar(outputColumns, new Object[]{value.get(j), global / total})));
                                }
                            }

                            return ret.iterator();
                        });
            }

            return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, outputColumns))
                    .generated(VERB, StreamType.Columnar, input)
                    .build(output);
        };
    }
}
