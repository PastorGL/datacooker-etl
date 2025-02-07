/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.operations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.math.config.KeyedMath;
import io.github.pastorgl.datacooker.math.functions.keyed.KeyedFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class KeyedMathOperation extends Operation {
    public static final String SOURCE_ATTR_PREFIX = "source_attr_";
    public static final String CALC_FUNCTION_PREFIX = "calc_function_";
    public static final String CALC_CONST_PREFIX = "calc_const_";
    private static final String CALC_RESULTS = "calc_results";

    private String[] sourceAttrs;
    private KeyedFunction[] keyedFunctions;
    private String[] resultingColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("keyedMath", "Perform a 'series' mathematical" +
                " function over a set of selected columns (treated as a Double) of a DataStream, under each unique key." +
                " Names of referenced attributes have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder(1)
                        .input("KeyValue DataStream with a set of attributes of type Double that comprise a series under each unique key",
                                StreamType.ATTRIBUTED
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_RESULTS, "List of resulting column names", Object[].class)
                        .dynDef(SOURCE_ATTR_PREFIX, "Column with Double values to use as series source", String.class)
                        .dynDef(CALC_FUNCTION_PREFIX, "The mathematical function to perform over the series", KeyedMath.class)
                        .dynDef(CALC_CONST_PREFIX, "An optional constant value for the selected function", Double.class)
                        .build(),

                new PositionalStreamsMetaBuilder(1)
                        .output("KeyValue DataStream with calculation result under each input series' key",
                                StreamType.COLUMNAR, StreamOrigin.GENERATED, null
                        )
                        .generated("*", "Resulting column names are defined by the operation parameter '" + CALC_RESULTS + "'")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        resultingColumns = Arrays.stream((Object[]) params.get(CALC_RESULTS)).map(String::valueOf).toArray(String[]::new);

        sourceAttrs = new String[resultingColumns.length];
        keyedFunctions = new KeyedFunction[resultingColumns.length];
        for (int i = resultingColumns.length - 1; i >= 0; i--) {
            String column = resultingColumns[i];

            sourceAttrs[i] = params.get(SOURCE_ATTR_PREFIX + column);

            KeyedMath keyedMath = params.get(CALC_FUNCTION_PREFIX + column);
            Double _const = params.get(CALC_CONST_PREFIX + column);
            try {
                keyedFunctions[i] = keyedMath.function(_const);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unable to instantiate requested function of '" + meta.verb + "'", e);
            }
        }
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final String[] _calcColumn = sourceAttrs;
        final List<String> _resultingColumns = Arrays.asList(resultingColumns);
        final int r = resultingColumns.length;
        final KeyedFunction[] _keyedFunctions = keyedFunctions;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            JavaPairRDD<Object, DataRecord<?>> out = input.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Double[]>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> row = it.next();

                            Double[] src = new Double[r];
                            for (int j = 0; j < r; j++) {
                                src[j] = row._2.asDouble(_calcColumn[j]);
                            }

                            ret.add(new Tuple2<>(row._1, src));
                        }

                        return ret.iterator();
                    })
                    .combineByKey(
                            t -> {
                                List<Double[]> ret = new ArrayList<>();
                                ret.add(t);
                                return ret;
                            },
                            (l, t) -> {
                                l.add(t);
                                return l;
                            },
                            (l1, l2) -> {
                                l1.addAll(l2);
                                return l1;
                            }
                    )
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, List<Double[]>> src = it.next();

                            Columnar rec = new Columnar(_resultingColumns);
                            for (int j = 0; j < r; j++) {
                                rec.put(_resultingColumns.get(j), _keyedFunctions[j].calcSeries(src._2, j));
                            }

                            ret.add(new Tuple2<>(src._1, rec));
                        }

                        return ret.iterator();
                    });

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), Collections.singletonMap(VALUE, _resultingColumns))
                    .generated(meta.verb, StreamType.Columnar, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
