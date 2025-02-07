/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.NamedStreamsMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class SplitByAttrsOperation extends Operation {
    public static final String SPLIT_TEMPLATE = "split_template";
    public static final String SPLIT_ATTRS = "split_attrs";

    public static final String OUTPUT_TEMPLATE = "template";
    static final String OUTPUT_SPLITS = "distinct_splits";

    private String outputNameTemplate;
    private String outputDistinctSplits;

    private Object[] splitAttrs;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("splitByAttrs", "Split a source DataStream it into several" +
                " partial DataStreams by values of selected attributes. Generated outputs are named by 'template'" +
                " that references encountered unique values of selected attributes",

                new PositionalStreamsMetaBuilder(1)
                        .input("Source DataStream to split into different outputs",
                                StreamType.ATTRIBUTED
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SPLIT_ATTRS, "Attributes to split the DataStream by their unique value combinations", Object[].class)
                        .def(SPLIT_TEMPLATE, "Format string for output names' wildcard part. Must contain all split attributes in form of '\\{split_attr\\}'")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .mandatoryOutput(OUTPUT_TEMPLATE, "Output name template. Must contain an wildcard mark * to be replaced by format string, i.e. output_*",
                                StreamType.ATTRIBUTED, StreamOrigin.FILTERED, null
                        )
                        .optionalOutput(OUTPUT_SPLITS, "Optional output that contains all of the distinct split attributes'" +
                                        " value combinations occurred in the input data",
                                StreamType.COLUMNAR, StreamOrigin.GENERATED, null
                        )
                        .generated(OUTPUT_SPLITS, "*", "Generated columns have same names as split attributes")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        String splitTemplate = outputStreams.get(OUTPUT_TEMPLATE);
        if (!splitTemplate.contains("*")) {
            throw new InvalidConfigurationException("Output name template for Operation '" + meta.verb + "' must contain an wildcard mark *");
        }

        outputNameTemplate = splitTemplate
                .replace("*", params.get(SPLIT_TEMPLATE));

        splitAttrs = params.get(SPLIT_ATTRS);

        for (Object attr : splitAttrs) {
            if (!outputNameTemplate.contains("{" + attr + "}")) {
                throw new InvalidConfigurationException("Split output name template '" + outputNameTemplate + "' must include split attribute reference {"
                        + attr + "} for the Operation '" + meta.verb + "'");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListOrderedMap<String, DataStream> execute() {
        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();

        DataStream input = inputStreams.getValue(0);
        input.surpassUsages();

        final List<String> _splitColumnNames = Arrays.stream(splitAttrs).map(String::valueOf).collect(Collectors.toList());

        JavaPairRDD<Object, DataRecord<?>> distinctSplits = input.rdd()
                .mapPartitionsToPair(it -> {
                    Set<Tuple2<Object, DataRecord<?>>> ret = new HashSet<>();

                    while (it.hasNext()) {
                        DataRecord<?> v = it.next()._2;

                        Columnar r = new Columnar(_splitColumnNames);
                        for (String col : _splitColumnNames) {
                            r.put(col, v.asIs(col));
                        }

                        ret.add(new Tuple2<>(r.hashCode(), r));
                    }

                    return ret.iterator();
                })
                .distinct();

        if (outputStreams.containsKey(OUTPUT_SPLITS)) {
            outputs.put(outputStreams.get(OUTPUT_SPLITS), new DataStreamBuilder(outputStreams.get(OUTPUT_SPLITS), Collections.singletonMap(VALUE, _splitColumnNames))
                    .generated(meta.verb, StreamType.Columnar, input)
                    .build(distinctSplits)
            );
        }

        Map<Object, DataRecord<?>> uniques = distinctSplits
                .collectAsMap();

        for (Map.Entry<Object, DataRecord<?>> u : uniques.entrySet()) {
            Columnar uR = (Columnar) u.getValue();
            String splitName = outputNameTemplate;
            for (String col : _splitColumnNames) {
                splitName = splitName.replace("{" + col + "}", uR.asString(col));
            }

            int hash = (Integer) u.getKey();
            JavaPairRDD<Object, DataRecord<?>> split = input.rdd().mapPartitionsToPair(it -> {
                List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Tuple2<Object, DataRecord<?>> v = it.next();

                    Columnar r = new Columnar(_splitColumnNames);
                    for (String col : _splitColumnNames) {
                        r.put(col, v._2.asIs(col));
                    }

                    if (r.hashCode() == hash) {
                        ret.add(v);
                    }
                }

                return ret.iterator();
            });

            outputs.put(splitName, new DataStreamBuilder(splitName, input.attributes())
                    .filtered(meta.verb, input)
                    .build(split)
            );
        }

        return outputs;
    }
}
