/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.populations.functions.MedianCalcFunction;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class FrequencyOperation extends Operation {
    static final String FREQUENCY_ATTR = "frequency_attr";
    static final String REFERENCE_ATTR = "reference_attr";

    static final String GEN_FREQUENCY = "_frequency";

    private String freqAttr;
    private String refAttr;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("frequency", "Statistical indicator for the Median Frequency of a value occurring" +
                " in the selected attribute per reference, which can be record key or another attribute." +
                " Names of referenced attributes have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("INPUT DataStream with attribute to count Median Frequency",
                                StreamType.SIGNAL
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(FREQUENCY_ATTR, "Attribute to count value frequencies per reference")
                        .def(REFERENCE_ATTR, "A reference attribute to use instead of record key",
                                null, "By default, use record key")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Output is Columnar with key for value and its Median Frequency in the record",
                                new StreamType[]{StreamType.Columnar}, StreamOrigin.GENERATED, null
                        )
                        .generated(GEN_FREQUENCY, "Generated column containing calculated Median Frequency")
                        .build()
        );
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        freqAttr = params.get(FREQUENCY_ATTR);

        refAttr = params.get(REFERENCE_ATTR);
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        String _ref = refAttr;
        String _freq = freqAttr;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, Double> valueToFreq = input.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> row = it.next();

                            Object key = (_ref == null) ? row._1 : row._2.asString(_ref);
                            Object value = row._2.asIs(_freq);

                            ret.add(new Tuple2<>(key, value));
                        }

                        return ret.iterator();
                    })
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
                            Tuple2<Object, Tuple2<Map<Object, Long>, Long>> t = it.next();

                            t._2._1.forEach((value, count) -> ret.add(new Tuple2<>(value, count.doubleValue() / t._2._2.doubleValue())));
                        }

                        return ret.iterator();
                    });

            final List<String> outputColumns = Collections.singletonList(GEN_FREQUENCY);

            JavaPairRDD<Object, DataRecord<?>> out = new MedianCalcFunction().call(valueToFreq)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Double> t = it.next();

                            ret.add(new Tuple2<>(t._1.toString(), new Columnar(outputColumns, new Object[]{t._2})));
                        }

                        return ret.iterator();
                    });

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), Collections.singletonMap(VALUE, outputColumns))
                    .generated(meta.verb, StreamType.Columnar, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
