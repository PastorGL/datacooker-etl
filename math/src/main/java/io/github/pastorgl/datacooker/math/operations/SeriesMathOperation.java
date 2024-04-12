/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.operations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.math.config.SeriesMath;
import io.github.pastorgl.datacooker.math.functions.series.SeriesFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class SeriesMathOperation extends Operation {
    public static final String CALC_ATTR = "calc_attr";
    public static final String CALC_FUNCTION = "calc_function";
    public static final String CALC_CONST = "calc_const";

    public static final String GEN_RESULT = "_result";

    private String calcColumn;

    private SeriesFunction seriesFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("seriesMath", "Calculate a 'series' mathematical function" +
                " over all values in a set record attribute, which is treated as a Double." +
                " Name of referenced attribute have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("DataStream with an attribute of type Double", StreamType.ATTRIBUTED)
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_ATTR, "Attribute to use as series source")
                        .def(CALC_FUNCTION, "The series function to perform", SeriesMath.class)
                        .def(CALC_CONST, "An optional ceiling value for the NORMALIZE function", Double.class,
                                100.D, "Default is '100 percent'")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream augmented with calculation result property", StreamType.ATTRIBUTED, StreamOrigin.AUGMENTED, null)
                        .generated(GEN_RESULT, "Generated property with a result of the series function")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        calcColumn = params.get(CALC_ATTR);
        SeriesMath seriesMath = params.get(CALC_FUNCTION);
        Double calcConst = params.get(CALC_CONST);

        try {
            seriesFunc = seriesMath.function(calcColumn, calcConst);
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to instantiate requested function of 'seriesMath'", e);
        }
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final String _calcColumn = calcColumn;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, Record<?>> inputRDD = input.rdd;

            JavaDoubleRDD series = inputRDD
                    .mapPartitionsToDouble(it -> {
                        List<Double> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Record<?> row = it.next()._2;

                            ret.add(row.asDouble(_calcColumn));
                        }
                        return ret.iterator();
                    });
            seriesFunc.calcSeries(series);

            JavaPairRDD<Object, Record<?>> out = inputRDD.mapPartitionsToPair(seriesFunc);

            Map<String, List<String>> outColumns = new HashMap<>(input.accessor.attributes());
            List<String> valueColumns = new ArrayList<>(outColumns.get(OBJLVL_VALUE));
            valueColumns.add(GEN_RESULT);
            outColumns.put(OBJLVL_VALUE, valueColumns);

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), input.streamType, outColumns)
                    .augmented(meta.verb, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
