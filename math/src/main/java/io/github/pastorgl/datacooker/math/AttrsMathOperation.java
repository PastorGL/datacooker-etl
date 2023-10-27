/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.math.config.AttrsMath;
import io.github.pastorgl.datacooker.math.functions.attrs.AttrsFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class AttrsMathOperation extends Operation {
    public static final String SOURCE_ATTRS_PREFIX = "source_attrs_";
    public static final String CALC_FUNCTION_PREFIX = "calc_function_";
    private static final String CALC_RESULTS = "calc_results";

    private AttrsFunction[] attrsFunctions;
    private String[] resultingAttrs;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("attrsMath", "For every source DataStream, perform one of the predefined mathematical" +
                " operations on selected sets of attributes inside each input record, generating attributes with results." +
                " Data type is implied Double. Names of referenced attributes have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("DataStream with attributes of type Double", StreamType.ATTRIBUTED)
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_RESULTS, "Names of resulting attributes", String[].class)
                        .dynDef(CALC_FUNCTION_PREFIX, "Mathematical function for a resulting attribute", AttrsMath.class)
                        .dynDef(SOURCE_ATTRS_PREFIX, "Set of source attributes a resulting attribute", String[].class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream with calculation results", StreamType.ATTRIBUTED, StreamOrigin.AUGMENTED, null)
                        .generated("*", "Names of generated attributes come from '" + CALC_RESULTS + "' parameter")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        resultingAttrs = params.get(CALC_RESULTS);

        attrsFunctions = new AttrsFunction[resultingAttrs.length];
        for (int i = resultingAttrs.length - 1; i >= 0; i--) {
            String column = resultingAttrs[i];

            String[] sourceColumns = params.get(SOURCE_ATTRS_PREFIX + column);

            AttrsMath attrsMath = params.get(CALC_FUNCTION_PREFIX + column);
            try {
                attrsFunctions[i] = attrsMath.function(sourceColumns);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unable to instantiate requested function of 'attrsMath'", e);
            }
        }
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final List<String> _resultingColumns = Arrays.asList(resultingAttrs);
        final AttrsFunction[] _attrsFunctions = attrsFunctions;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            JavaPairRDD<Object, Record<?>> out = input.rdd.mapPartitionsToPair(it -> {
                List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Tuple2<Object, Record<?>> rec = it.next();

                    Record<?> r = (Record<?>) rec._2.clone();
                    for (int j = 0; j < _attrsFunctions.length; j++) {
                        r.put(_resultingColumns.get(j), _attrsFunctions[j].calcValue(r));
                    }

                    ret.add(new Tuple2<>(rec._1, r));
                }

                return ret.iterator();
            });

            final List<String> outputColumns = new ArrayList<>(input.accessor.attributes(OBJLVL_VALUE));
            outputColumns.addAll(_resultingColumns);
            Map<String, List<String>> columns = new HashMap<>(input.accessor.attributes());
            columns.put(OBJLVL_VALUE, outputColumns);

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), input.streamType, columns)
                    .augmented(meta.verb, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
