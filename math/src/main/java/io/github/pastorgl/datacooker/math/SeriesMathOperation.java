/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.math.config.SeriesMath;
import io.github.pastorgl.datacooker.math.functions.series.SeriesFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class SeriesMathOperation extends Operation {
    public static final String CALC_COLUMN = "calc_column";
    public static final String CALC_FUNCTION = "calc_function";
    public static final String CALC_CONST = "calc_const";

    public static final String GEN_RESULT = "_result";

    private String calcColumn;

    private SeriesFunction seriesFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("seriesMath", "Calculate a 'series' mathematical function" +
                " over all values in a set record attribute, which is treated as a Double",

                new PositionalStreamsMetaBuilder()
                        .input("DataStream with an attribute of type Double",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_COLUMN, "Attribute to use as series source")
                        .def(CALC_FUNCTION, "The series function to perform", SeriesMath.class)
                        .def(CALC_CONST, "An optional ceiling value for the NORMALIZE function", Double.class,
                                100.D, "Default is '100 percent'")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream augmented with calculation result property",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_RESULT, "Generated property with a result of the series function")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        calcColumn = params.get(CALC_COLUMN);
        SeriesMath seriesMath = params.get(CALC_FUNCTION);
        Double calcConst = params.get(CALC_CONST);

        try {
            seriesFunc = seriesMath.function(calcColumn, calcConst);
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to instantiate requested function of 'seriesMath'", e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _calcColumn = calcColumn;

        DataStream input = inputStreams.getValue(0);
        JavaRDD<Object> inputRDD = (JavaRDD<Object>) input.get();
        JavaDoubleRDD series = inputRDD
                .mapPartitionsToDouble(it -> {
                    List<Double> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        ret.add(row.asDouble(_calcColumn));
                    }
                    return ret.iterator();
                });

        seriesFunc.calcSeries(series);
        JavaRDD<Object> output = inputRDD.mapPartitions(seriesFunc);

        Map<String, List<String>> outColumns = new HashMap<>(input.accessor.attributes());
        List<String> valueColumns = new ArrayList<>(outColumns.get(OBJLVL_VALUE));
        valueColumns.add(GEN_RESULT);
        outColumns.put(OBJLVL_VALUE, valueColumns);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, outColumns));
    }

}
