/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.math.config.AttrsMath;
import io.github.pastorgl.datacooker.math.functions.attrs.AttrsFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class AttrsMathOperation extends Operation {
    public static final String SOURCE_COLUMN_PREFIX = "source_attrs_";
    public static final String CALC_FUNCTION_PREFIX = "calc_function_";
    private static final String CALC_RESULTS = "calc_results";

    private AttrsFunction[] attrsFunctions;
    private String[] resultingColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("attrsMath", "This operation performs one of the predefined mathematical" +
                " operations on selected sets of attributes inside each input row, generating attributes with results." +
                " Data type is implied Double",

                new PositionalStreamsMetaBuilder()
                        .input("DataStream with attributes of type Double",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Track, StreamType.Polygon}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_RESULTS, "Attributes with results", String[].class)
                        .dynDef(CALC_FUNCTION_PREFIX, "The mathematical function to perform", AttrsMath.class)
                        .dynDef(SOURCE_COLUMN_PREFIX, "Set of source attributes for each of calculation results", String[].class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream with calculation results",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Track, StreamType.Polygon},
                                Origin.AUGMENTED, null
                        )
                        .generated("*", "Names of generated attributes come from '" + CALC_RESULTS + "' parameter")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        resultingColumns = params.get(CALC_RESULTS);

        attrsFunctions = new AttrsFunction[resultingColumns.length];
        for (int i = resultingColumns.length - 1; i >= 0; i--) {
            String column = resultingColumns[i];

            String[] sourceColumns = params.get(SOURCE_COLUMN_PREFIX + column);

            AttrsMath attrsMath = params.get(CALC_FUNCTION_PREFIX + column);
            try {
                attrsFunctions[i] = attrsMath.function(sourceColumns);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unable to instantiate requested function of 'attrsMath'", e);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, DataStream> execute() {
        DataStream input = inputStreams.getValue(0);

        final List<String> _resultingColumns = Arrays.asList(resultingColumns);
        final int r = resultingColumns.length;
        final AttrsFunction[] _attrsFunctions = attrsFunctions;

        final List<String> outputColumns = new ArrayList<>(input.accessor.attributes(OBJLVL_VALUE));
        outputColumns.addAll(_resultingColumns);

        JavaRDDLike output;

        if (input.streamType == StreamType.Columnar) {
            output = ((JavaRDD<Columnar>) input.get())
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar rec = new Columnar(outputColumns);
                            rec.put(it.next().asIs());

                            for (int i = 0; i < _attrsFunctions.length; i++) {
                                rec.put(_resultingColumns.get(i), _attrsFunctions[i].calcValue(rec));
                            }

                            ret.add(rec);
                        }

                        return ret.iterator();
                    });
        } else {
            output = ((JavaRDD<Object>) input.get())
                    .mapPartitions(it -> {
                        List<Object> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            SpatialRecord rec = (SpatialRecord) ((SpatialRecord) it.next()).clone();
                            for (int i = 0; i < _attrsFunctions.length; i++) {
                                rec.put(_resultingColumns.get(i), _attrsFunctions[i].calcValue(rec));
                            }

                            ret.add(rec);
                        }

                        return ret.iterator();
                    });
        }

        Map<String, List<String>> columns = new HashMap<>(input.accessor.attributes());
        columns.put(OBJLVL_VALUE, outputColumns);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, columns));
    }
}
