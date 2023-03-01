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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PercentRankIncOperation extends Operation {
    static final String VALUE_COLUMN = "value_column";
    static final String GEN_VALUE = "_value";
    static final String GEN_RANK = "_rank";
    protected String valueColumn;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("percentRankInc", "Statistical indicator for 'percentile rank inclusive'" +
                " function for a Double input value column. Output is fixed to value then rank attributes. Does not work" +
                " with datasets consisting of less than one element, and returns NaN for single-element dataset",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar or Pair RDD with value column to calculate the rank",
                                new StreamType[]{StreamType.Columnar, StreamType.KeyValue}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(VALUE_COLUMN, "Column for counting rank values, must be of type Double")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("For KeyValue DataStream rank is calculated for all values under same key and put under that key," +
                                        " for Columnar for the entire DataStream, and output is same type as input",
                                new StreamType[]{StreamType.Columnar, StreamType.KeyValue}, Origin.GENERATED, null
                        )
                        .generated(GEN_VALUE, "Ranked value")
                        .generated(GEN_RANK, "Calculated rank")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        valueColumn = params.get(VALUE_COLUMN);
    }

    @Override
    public Map<String, DataStream> execute() {
        String _valueColumn = valueColumn;

        DataStream input = inputStreams.getValue(0);
        JavaRDDLike output;

        final List<String> outputColumns = Arrays.asList(GEN_VALUE, GEN_RANK);

        if (input.streamType == StreamType.Columnar) {
            JavaPairRDD<Double, Long> valueCounts = ((JavaRDD<Columnar>) input.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Double, Long>> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Columnar row = it.next();

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

                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Double, Long> value = it.next();

                            Columnar acc = new Columnar(outputColumns, new Object[]{value._1, global / total});
                            for (int j = 0; j < value._2; j++) {
                                ret.add(acc);
                            }

                            global += value._2;
                        }

                        return ret.iterator();
                    }, true);
        } else {
            output = ((JavaPairRDD<String, Columnar>) input.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Double>> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Tuple2<String, Columnar> t = it.next();

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
                        List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, ArrayList<Double>> next = it.next();

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

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, Collections.singletonMap(OBJLVL_VALUE, outputColumns)));
    }
}
