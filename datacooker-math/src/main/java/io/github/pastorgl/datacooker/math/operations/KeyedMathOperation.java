/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.operations;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.math.config.KeyedMath;
import io.github.pastorgl.datacooker.math.functions.keyed.KeyedFunction;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class KeyedMathOperation extends Transformer {
    public static final String SOURCE_ATTR_PREFIX = "source_attr_";
    public static final String CALC_FUNCTION_PREFIX = "calc_function_";
    public static final String CALC_CONST_PREFIX = "calc_const_";
    private static final String CALC_RESULTS = "calc_results";
    static final String VERB = "keyedMath";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Perform a 'series' mathematical" +
                " function over a set of selected columns (treated as a Double) of a DataStream, under each unique key." +
                " Names of referenced attributes have to be same in each INPUT DataStream")
                .operation()
                .input(StreamType.ATTRIBUTED,
                        "DataStream with a set of attributes of type Double that comprise a series under each unique key")
                .def(CALC_RESULTS, "List of resulting column names", Object[].class)
                .dynDef(SOURCE_ATTR_PREFIX, "Column with Double values to use as series source", String.class)
                .dynDef(CALC_FUNCTION_PREFIX, "The mathematical function to perform over the series", KeyedMath.class)
                .dynDef(CALC_CONST_PREFIX, "An optional constant value for the selected function", Double.class)
                .output(StreamType.COLUMNAR, "KeyValue DataStream with calculation result under each input series' key",
                        StreamOrigin.GENERATED, null)
                .generated("*", "Resulting column names are defined by the operation parameter '" + CALC_RESULTS + "'")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (input, ignore, params) -> {
            String[] resultingColumns = Arrays.stream((Object[]) params.get(CALC_RESULTS)).map(String::valueOf).toArray(String[]::new);

            final String[] sourceAttrs = new String[resultingColumns.length];
            final KeyedFunction[] keyedFunctions = new KeyedFunction[resultingColumns.length];
            for (int i = resultingColumns.length - 1; i >= 0; i--) {
                String column = resultingColumns[i];

                sourceAttrs[i] = params.get(SOURCE_ATTR_PREFIX + column);

                KeyedMath keyedMath = params.get(CALC_FUNCTION_PREFIX + column);
                Double _const = params.get(CALC_CONST_PREFIX + column);
                try {
                    keyedFunctions[i] = keyedMath.function(_const);
                } catch (Exception ignored) {
                }
            }

            int r = sourceAttrs.length;
            final List<String> newColumns = Arrays.asList(resultingColumns);

            JavaPairRDD<Object, DataRecord<?>> out = input.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Double[]>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> row = it.next();

                            Double[] src = new Double[r];
                            for (int j = 0; j < r; j++) {
                                src[j] = row._2.asDouble(sourceAttrs[j]);
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

                            Columnar rec = new Columnar(newColumns);
                            for (int j = 0; j < r; j++) {
                                rec.put(resultingColumns[j], keyedFunctions[j].calcSeries(src._2, j));
                            }

                            ret.add(new Tuple2<>(src._1, rec));
                        }

                        return ret.iterator();
                    });

            return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, newColumns))
                    .generated(VERB, StreamType.Columnar, input)
                    .build(out);
        };
    }
}
