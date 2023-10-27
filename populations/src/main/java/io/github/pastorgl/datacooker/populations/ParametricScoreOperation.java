/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.IntStream;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ParametricScoreOperation extends Operation {
    public static final String RDD_INPUT_VALUES = "values";
    public static final String RDD_INPUT_MULTIPLIERS = "multipliers";

    public static final String COUNT_ATTR = "count_attr";
    public static final String VALUE_ATTR = "value_attr";
    public static final String GROUPING_ATTR = "grouping_attr";
    public static final String MATCH_ATTR = "match_attr";
    public static final String MULTIPLIER_ATTR = "multiplier_attr";

    public final static String GEN_SCORE_PREFIX = "_score_";
    public final static String GEN_VALUE_PREFIX = "_value_";
    public static final String TOP_SCORES = "top_scores";

    private String value;
    private String group;
    private String count;

    private String match;
    private String multiplier;

    private Integer top;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("parametricScore", "Calculate a top of Parametric Scores for a value by its count and multiplier",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(RDD_INPUT_VALUES, "Values to group and count scores",
                                StreamType.SIGNAL
                        )
                        .mandatoryInput(RDD_INPUT_MULTIPLIERS, "Value multipliers for scores",
                                new StreamType[]{StreamType.Columnar, StreamType.Structured}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(GROUPING_ATTR, "Attribute for grouping count attributes per value attribute values")
                        .def(VALUE_ATTR, "Attribute for counting unique values per other attribute", String.class,
                                null, "By default, use record key as value")
                        .def(COUNT_ATTR, "Attribute to count unique values of other attribute")
                        .def(MATCH_ATTR, "Attribute to match multiplier with counting attribute", String.class,
                                null, "By default, use record key as match value")
                        .def(MULTIPLIER_ATTR, "Attribute with Double multiplier")
                        .def(TOP_SCORES, "How long is the top scores list", Integer.class,
                                1, "By default, generate only the topmost score")
                        .build(),

                new PositionalStreamsMetaBuilder(1)
                        .output("Parametric scores Columnar OUTPUT, with grouping attribute value as record key",
                                new StreamType[]{StreamType.Columnar}, StreamOrigin.GENERATED, Collections.singletonList(RDD_INPUT_VALUES)
                        )
                        .generated(GEN_VALUE_PREFIX + "*", "Generated attributes with value have numeric postfix starting with 1")
                        .generated(GEN_SCORE_PREFIX + "*", "Generated attributes with score have numeric postfix starting with 1")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        top = params.get(TOP_SCORES);

        group = params.get(GROUPING_ATTR);
        value = params.get(VALUE_ATTR);
        count = params.get(COUNT_ATTR);

        match = params.get(MATCH_ATTR);
        multiplier = params.get(MULTIPLIER_ATTR);
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        final String _match = match;
        final String _multiplier = multiplier;

        Map<Object, Double> multipliers = inputStreams.get(RDD_INPUT_MULTIPLIERS).rdd
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> next = it.next();

                        Object match = (_match == null) ? next._1 : next._2.asIs(_match);
                        ret.add(new Tuple2<>(match, next._2.asDouble(_multiplier)));
                    }

                    return ret.iterator();
                })
                .collectAsMap();

        final String _group = group;
        final String _value = value;
        final String _count = count;

        DataStream inputValues = inputStreams.get(RDD_INPUT_VALUES);
        JavaPairRDD<Object, Tuple3<Object, Object, Long>> countGroupValues = inputValues.rdd
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Tuple3<Object, Object, Object>, Long>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> row = it.next();

                        Object count = row._2.asIs(_count);
                        Object group = row._2.asIs(_group);
                        Object value = (_value == null) ? row._1 : row._2.asIs(_value);

                        ret.add(new Tuple2<>(new Tuple3<>(count, group, value), 1L));
                    }

                    return ret.iterator();
                })
                .reduceByKey(Long::sum)
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Tuple3<Object, Object, Long>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Tuple3<Object, Object, Object>, Long> t = it.next();

                        ret.add(new Tuple2<>(t._1._1(), new Tuple3<>(t._1._2(), t._1._3(), t._2)));
                    }

                    return ret.iterator();
                });

        final int _top = top;
        final List<String> outputColumns = new ArrayList<>();
        IntStream.rangeClosed(1, top).forEach(i -> {
            outputColumns.add(GEN_SCORE_PREFIX + i);
            outputColumns.add(GEN_VALUE_PREFIX + i);
        });

        Broadcast<HashMap<Object, Double>> mult = JavaSparkContext.fromSparkContext(countGroupValues.context()).broadcast(new HashMap<>(multipliers));
        JavaPairRDD<Object, Record<?>> output = countGroupValues
                .mapPartitionsToPair(it -> {
                    HashMap<Object, Double> mults = mult.getValue();

                    List<Tuple2<Tuple2<Object, Object>, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Tuple3<Object, Object, Long>> t = it.next();

                        if (mults.containsKey(t._1)) {
                            ret.add(new Tuple2<>(new Tuple2<>(t._2._1(), t._2._2()), mults.get(t._1) * t._2._3()));
                        }
                    }

                    return ret.iterator();
                })
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
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Map<Double, Object>> t = it.next();

                        Columnar rec = new Columnar(outputColumns);
                        Map<Double, Object> resortMap = new TreeMap<>(Comparator.reverseOrder());
                        resortMap.putAll(t._2);
                        List<Map.Entry<Double, Object>> r = new ArrayList<>(resortMap.entrySet());
                        while (r.size() < _top) {
                            r.add(new AbstractMap.SimpleEntry<>(null, null));
                        }
                        for (int i = 1; i <= _top; i++) {
                            rec.put(GEN_VALUE_PREFIX + i, r.get(i - 1).getValue());
                            rec.put(GEN_SCORE_PREFIX + i, r.get(i - 1).getKey());
                        }

                        ret.add(new Tuple2<>(t._1, rec));
                    }

                    return ret.iterator();
                });

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        outputs.put(outputStreams.firstKey(), new DataStreamBuilder(outputStreams.firstKey(), StreamType.Columnar, Collections.singletonMap(OBJLVL_VALUE, outputColumns))
                .generated(meta.verb, inputValues)
                .build(output)
        );
        return outputs;
    }
}
