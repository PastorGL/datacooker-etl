/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.math.config.KeyedMath;
import io.github.pastorgl.datacooker.math.functions.keyed.KeyedFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class KeyedMathOperation extends Operation {

    public static final String SOURCE_COLUMN_PREFIX = "source_column_";
    public static final String CALC_FUNCTION_PREFIX = "calc_function_";
    public static final String CALC_CONST_PREFIX = "calc_const_";
    private static final String CALC_RESULTS = "calc_results";

    private String[] sourceColumns;
    private KeyedFunction[] keyedFunctions;
    private String[] resultingColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("keyedMath", "Perform a 'series' mathematical" +
                " function over a set of selected columns (treated as a Double) of a KeyValue DataStream, under each unique key",

                new PositionalStreamsMetaBuilder()
                        .input("KeyValue DataStream with a set of attributes of type Double that comprise a series under each unique key",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_RESULTS, "List of resulting column names", String[].class)
                        .dynDef(SOURCE_COLUMN_PREFIX, "Column with Double values to use as series source", String.class)
                        .dynDef(CALC_FUNCTION_PREFIX, "The mathematical function to perform over the series", KeyedMath.class)
                        .dynDef(CALC_CONST_PREFIX, "An optional constant value for the selected function", Double.class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("KeyValue DataStream with calculation result under each input series' key",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, null
                        )
                        .generated("*", "Resulting column names are defined by the operation parameter '" + CALC_RESULTS + "'")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        resultingColumns = params.get(CALC_RESULTS);

        sourceColumns = new String[resultingColumns.length];
        keyedFunctions = new KeyedFunction[resultingColumns.length];
        for (int i = resultingColumns.length - 1; i >= 0; i--) {
            String column = resultingColumns[i];

            sourceColumns[i] = params.get(SOURCE_COLUMN_PREFIX + column);

            KeyedMath keyedMath = params.get(CALC_FUNCTION_PREFIX + column);
            Double _const = params.get(CALC_CONST_PREFIX + column);
            try {
                keyedFunctions[i] = keyedMath.function(_const);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unable to instantiate requested function of 'keyedMath'", e);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        JavaPairRDD<String, Columnar> inputRDD = (JavaPairRDD<String, Columnar>) inputStreams.getValue(0).get();

        final String[] _calcColumn = sourceColumns;
        final List<String> _resultingColumns = Arrays.asList(resultingColumns);
        final int r = resultingColumns.length;
        final KeyedFunction[] _keyedFunctions = keyedFunctions;

        JavaPairRDD<String, Columnar> output = inputRDD
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Double[]>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, Columnar> row = it.next();

                        Double[] src = new Double[r];
                        for (int i = 0; i < r; i++) {
                            src[i] = row._2.asDouble(_calcColumn[i]);
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
                    List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<String, List<Double[]>> src = it.next();

                        Columnar rec = new Columnar(_resultingColumns);
                        for (int i = 0; i < r; i++) {
                            rec.put(_resultingColumns.get(i), _keyedFunctions[i].calcSeries(src._2, i));
                        }

                        ret.add(new Tuple2<>(src._1, rec));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.KeyValue, output, Collections.singletonMap(OBJLVL_VALUE, _resultingColumns)));
    }
}
