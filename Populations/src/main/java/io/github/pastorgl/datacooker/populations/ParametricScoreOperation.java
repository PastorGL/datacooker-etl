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
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.IntStream;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ParametricScoreOperation extends Operation {
    public static final String RDD_INPUT_VALUES = "values";
    public static final String RDD_INPUT_MULTIPLIERS = "multipliers";

    public static final String COUNT_ATTR = "count_attr";
    public static final String VALUE_ATTR = "value_attr";
    public static final String GROUPING_ATTR = "grouping_attr";
    public static final String MULTIPLIER_COLUMN = "multiplier_column";

    public final static String GEN_SCORE_PREFIX = "_score_";
    public final static String GEN_VALUE_PREFIX = "_value_";
    public static final String TOP_SCORES = "top_scores";

    private String valueColumn;
    private String groupColumn;
    private String countColumn;

    private String multiplierColumn;

    private Integer top;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("parametricScore", "Calculate a top of parametric scores for a value by its count and multiplier",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(RDD_INPUT_VALUES, "Values to group and count scores",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .mandatoryInput(RDD_INPUT_MULTIPLIERS, "Value multipliers for scores. Key is value to match",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(GROUPING_ATTR, "Column for grouping count attributes per value column values")
                        .def(VALUE_ATTR, "Column for counting unique values per other column")
                        .def(COUNT_ATTR, "Column to count unique values of other column")
                        .def(MULTIPLIER_COLUMN, "Column with Double multiplier")
                        .def(TOP_SCORES, "How long is the top scores list", Integer.class,
                                1, "By default, generate only the topmost score")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Parametric scores output",
                                new StreamType[]{StreamType.Columnar}, Origin.GENERATED, Collections.singletonList(RDD_INPUT_VALUES)
                        )
                        .generated("*", "Column with grouping ID from '" + RDD_INPUT_VALUES + "' input retains its name")
                        .generated(GEN_VALUE_PREFIX + "*", "Generated columns with value have numeric postfix starting with 1")
                        .generated(GEN_SCORE_PREFIX + "*", "Generated columns with score have numeric postfix starting with 1")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        top = params.get(TOP_SCORES);

        groupColumn = params.get(GROUPING_ATTR);
        valueColumn = params.get(VALUE_ATTR);
        countColumn = params.get(COUNT_ATTR);

        multiplierColumn = params.get(MULTIPLIER_COLUMN);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _multiplierColumn = multiplierColumn;

        JavaPairRDD<String, Double> multipliers = ((JavaPairRDD<String, Columnar>) inputStreams.get(RDD_INPUT_MULTIPLIERS).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, Columnar> next = it.next();

                        ret.add(new Tuple2<>(next._1, next._2.asDouble(_multiplierColumn)));
                    }

                    return ret.iterator();
                });

        final String _groupColumn = groupColumn;
        final String _valueColumn = valueColumn;
        final String _countColumn = countColumn;

        JavaPairRDD<String, Tuple3<Object, Object, Long>> countGroupValues = ((JavaRDD<Object>) inputStreams.get(RDD_INPUT_VALUES).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Tuple3<String, Object, Object>, Long>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        String count = row.asString(_countColumn);
                        Object group = row.asIs(_groupColumn);
                        Object value = row.asIs(_valueColumn);

                        ret.add(new Tuple2<>(new Tuple3<>(count, group, value), 1L));
                    }

                    return ret.iterator();
                })
                .reduceByKey(Long::sum)
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Tuple3<Object, Object, Long>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Tuple3<String, Object, Object>, Long> t = it.next();

                        ret.add(new Tuple2<>(t._1._1(), new Tuple3<>(t._1._2(), t._1._3(), t._2)));
                    }

                    return ret.iterator();
                });

        final int _top = top;
        final List<String> outputColumns = new ArrayList<>();
        outputColumns.add(groupColumn);
        IntStream.rangeClosed(1, top).forEach(i -> {
            outputColumns.add(GEN_SCORE_PREFIX + i);
            outputColumns.add(GEN_VALUE_PREFIX + i);
        });

        JavaRDD<Columnar> output = countGroupValues.join(multipliers)
                .values()
                .mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1._1(), t._1._2()), t._2 * t._1._3()))
                .reduceByKey(Double::sum)
                .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)))
                .combineByKey(
                        v -> {
                            Map<Double, Object> r = new HashMap<>();
                            r.put(v._2, v._1);
                            return r;
                        },
                        (t, v) -> {
                            t.put(v._2, v._1);
                            return t;
                        },
                        (t1, t2) -> {
                            t1.putAll(t2);

                            return t1;
                        }
                )
                .mapPartitions(it -> {
                    List<Columnar> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Map<Double, Object>> t = it.next();

                        Columnar rec = new Columnar(outputColumns);
                        rec.put(_groupColumn, t._1);

                        Map<Double, Object> resortMap = new TreeMap<>(Comparator.reverseOrder());
                        resortMap.putAll(t._2);
                        List<Map.Entry<Double, Object>> r = new ArrayList<>(resortMap.entrySet());
                        for (int i = 1; i <= _top; i++) {
                            rec.put(GEN_VALUE_PREFIX + i, r.get(i - 1).getValue());
                            rec.put(GEN_SCORE_PREFIX + i, r.get(i - 1).getKey());
                        }

                        ret.add(rec);
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Columnar, output, Collections.singletonMap(OBJLVL_VALUE, outputColumns)));
    }
}
