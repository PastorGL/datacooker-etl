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
import io.github.pastorgl.datacooker.populations.functions.MedianCalcFunction;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class FrequencyOperation extends Operation {
    static final String FREQUENCY_COLUMN = "frequency_column";
    static final String REFERENCE_COLUMN = "reference_column";
    static final String DEF_KEY = "_key";
    static final String GEN_FREQUENCY = "_frequency";
    private String freqColumn;
    private String refColumn;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("frequency", "Statistical indicator for the median Frequency of a value occurring" +
                " in a column per reference (key or another column)",

                new PositionalStreamsMetaBuilder()
                        .input("KeyValue or Columnar DataStream",
                                new StreamType[]{StreamType.Columnar, StreamType.KeyValue}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(FREQUENCY_COLUMN, "Column to count value frequencies per reference")
                        .def(REFERENCE_COLUMN, "A reference column for Columnar DataStream",
                                DEF_KEY, "By default, reference column is assumed by name '" + DEF_KEY + "'")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Output is KeyValue with key for value and its Frequency in the record",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, null
                        )
                        .generated(GEN_FREQUENCY, "Generated column containing calculated Frequency")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        freqColumn = params.get(FREQUENCY_COLUMN);

        refColumn = params.get(REFERENCE_COLUMN);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        String _refColumn = refColumn;
        String _freqColumn = freqColumn;

        DataStream signals = inputStreams.getValue(0);

        JavaPairRDD<String, Object> refToValue;
        if (signals.streamType == StreamType.Columnar) {
            refToValue = ((JavaRDD<Columnar>) signals.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Object>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar row = it.next();

                            String key = row.asString(_refColumn);
                            Object value = row.asIs(_freqColumn);

                            ret.add(new Tuple2<>(key, value));
                        }

                        return ret.iterator();
                    });
        } else {
            refToValue = ((JavaPairRDD<String, Columnar>) signals.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Object>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, Columnar> row = it.next();

                            String key = row._1;
                            Object value = row._2.asIs(_freqColumn);

                            ret.add(new Tuple2<>(key, value));
                        }

                        return ret.iterator();
                    });
        }

        JavaPairRDD<Object, Double> valueToFreq = refToValue
                .combineByKey(
                        t -> {
                            Map<Object, Long> ret = Collections.singletonMap(t, 1L);
                            return new Tuple2<>(ret, 1L);
                        },
                        (c, t) -> {
                            HashMap<Object, Long> ret = new HashMap<>(c._1);
                            ret.compute(t, (k, v) -> (v == null) ? 1L : v + 1L);
                            return new Tuple2<>(ret, c._2 + 1L);
                        },
                        (c1, c2) -> {
                            HashMap<Object, Long> ret = new HashMap<>(c1._1);
                            c2._1.forEach((key, v2) -> ret.compute(key, (k, v1) -> (v1 == null) ? v2 : v1 + v2));

                            return new Tuple2<>(ret, c1._2 + c2._2);
                        }
                )
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        // key -> (freq -> count)..., total
                        Tuple2<String, Tuple2<Map<Object, Long>, Long>> t = it.next();

                        t._2._1.forEach((value, count) -> ret.add(new Tuple2<>(value, count.doubleValue() / t._2._2.doubleValue())));
                    }

                    return ret.iterator();
                });

        JavaRDD<Tuple2<Object, Double>> medianFreq = new MedianCalcFunction().call(valueToFreq);

        final List<String> outputColumns = Collections.singletonList(GEN_FREQUENCY);
        JavaPairRDD<String, Record> output = medianFreq
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Record>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Double> t = it.next();

                        ret.add(new Tuple2<>(t._1.toString(), new Columnar(outputColumns, new Object[]{t._2})));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.KeyValue, output, new SingletonMap<>(OBJLVL_VALUE, outputColumns)));
    }
}
