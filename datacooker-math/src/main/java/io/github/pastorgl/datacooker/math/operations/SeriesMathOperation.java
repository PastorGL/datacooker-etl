/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.operations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.math.config.SeriesMath;
import io.github.pastorgl.datacooker.math.functions.series.SeriesFunction;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.TransformerOperation;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class SeriesMathOperation extends TransformerOperation {
    public static final String CALC_ATTR = "calc_attr";
    public static final String CALC_FUNCTION = "calc_function";
    public static final String CALC_CONST = "calc_const";

    public static final String GEN_RESULT = "_result";
    static final String VERB = "seriesMath";

    private String calcColumn;

    private SeriesFunction seriesFunc;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Calculate a 'series' mathematical function" +
                " over all values in a set record attribute, which is treated as a Double." +
                " Name of referenced attribute have to be same in each INPUT DataStream")
                .operation()
                .input(StreamType.ATTRIBUTED, "DataStream with an attribute of type Double")
                .def(CALC_ATTR, "Attribute to use as series source")
                .def(CALC_FUNCTION, "The series function to perform", SeriesMath.class)
                .def(CALC_CONST, "An optional ceiling value for the NORMALIZE function", Double.class,
                        100.D, "Default is '100 percent'")
                .output(StreamType.ATTRIBUTED, "DataStream augmented with calculation result property",
                        StreamOrigin.AUGMENTED, null)
                .generated(GEN_RESULT, "Generated property with a result of the series function")
                .build();
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
    public StreamTransformer transformer() {
        final String _calcColumn = calcColumn;
        final SeriesFunction _seriesFunc = seriesFunc;

        return (input, name) -> {
            JavaPairRDD<Object, DataRecord<?>> inputRDD = input.rdd();

            JavaDoubleRDD series = inputRDD
                    .mapPartitionsToDouble(it -> {
                        List<Double> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            DataRecord<?> row = it.next()._2;

                            ret.add(row.asDouble(_calcColumn));
                        }
                        return ret.iterator();
                    })
                    .cache();
            _seriesFunc.calcSeries(series);

            JavaPairRDD<Object, DataRecord<?>> out = inputRDD.mapPartitionsToPair(_seriesFunc);

            Map<ObjLvl, List<String>> outColumns = new HashMap<>(input.attributes());
            List<String> valueColumns = new ArrayList<>(outColumns.get(VALUE));
            valueColumns.add(GEN_RESULT);
            outColumns.put(VALUE, valueColumns);

            return new DataStreamBuilder(name, outColumns)
                    .augmented(VERB, input)
                    .build(out);
        };
    }
}
