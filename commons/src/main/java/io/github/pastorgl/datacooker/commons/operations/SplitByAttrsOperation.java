/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class SplitByAttrsOperation extends Operation {
    public static final String SPLIT_TEMPLATE = "split_template";
    public static final String OUTPUT_SPLITS_TEMPLATE = "template";
    public static final String SPLIT_ATTRS = "split_attrs";
    static final String DISTINCT_SPLITS = "distinct_splits";

    private String outputNameTemplate;
    private String outputDistinctSplits;

    private String[] splitColumnNames;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("splitByAttrs", "Split a source DataStream it into several" +
                " partial DataStreams by values of selected attributes. Generated outputs are named by 'template'" +
                " that references encountered unique values of selected attributes",

                new PositionalStreamsMetaBuilder()
                        .input("Source DataStream to split into different outputs",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SPLIT_ATTRS, "Attributes to split the DataStream by their unique value combinations", String[].class)
                        .def(SPLIT_TEMPLATE, "Format string for output names' wildcard part. Must contain all split attributes in form of '\\{split_attr\\}'")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .mandatoryOutput(OUTPUT_SPLITS_TEMPLATE, "Output name template. Must contain an wildcard mark * to be replaced by format string, i.e. output_*",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}, Origin.FILTERED, null
                        )
                        .optionalOutput(DISTINCT_SPLITS, "Optional output that contains all of the distinct split attributes'" +
                                        " value combinations occurred in the input data",
                                new StreamType[]{StreamType.Columnar}, Origin.GENERATED, null
                        )
                        .generated(DISTINCT_SPLITS, "*", "Generated columns have same names as split attributes")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        String splitTemplate = outputStreams.get(OUTPUT_SPLITS_TEMPLATE);
        if (!splitTemplate.contains("*")) {
            throw new InvalidConfigurationException("Output name template for Operation '" + meta.verb + "' must contain an wildcard mark *");
        }

        outputNameTemplate = splitTemplate
                .replace("*", params.get(SPLIT_TEMPLATE));

        splitColumnNames = params.get(SPLIT_ATTRS);

        for (String col : splitColumnNames) {
            if (!outputNameTemplate.contains("{" + col + "}")) {
                throw new InvalidConfigurationException("Split output name template '" + outputNameTemplate + "' must include split column reference {"
                        + col + "} for the Operation '" + meta.verb + "'");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, DataStream> execute() {
        Map<String, DataStream> output = new HashMap<>();

        DataStream input = inputStreams.getValue(0);

        JavaRDD<Object> cachedInput = ((JavaRDD<Object>) input.get())
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        final List<String> _splitColumnNames = Arrays.stream(splitColumnNames).collect(Collectors.toList());

        JavaPairRDD<Integer, Columnar> distinctSplits = cachedInput
                .mapPartitionsToPair(it -> {
                    Set<Tuple2<Integer, Columnar>> ret = new HashSet<>();

                    while (it.hasNext()) {
                        Record v = (Record) it.next();

                        Columnar r = new Columnar(_splitColumnNames);
                        for (String col : _splitColumnNames) {
                            r.put(col, v.asIs(col));
                        }

                        ret.add(new Tuple2<>(r.hashCode(), r));
                    }

                    return ret.iterator();
                })
                .distinct();

        if (outputStreams.containsKey(DISTINCT_SPLITS)) {
            output.put(outputStreams.get(DISTINCT_SPLITS), new DataStream(StreamType.Columnar, distinctSplits.values(), Collections.singletonMap(OBJLVL_VALUE, _splitColumnNames)));
        }

        Map<Integer, Columnar> uniques = distinctSplits
                .collectAsMap();

        for (Map.Entry<Integer, Columnar> u : uniques.entrySet()) {
            Columnar uR = u.getValue();
            String splitName = outputNameTemplate;
            for (String col : _splitColumnNames) {
                splitName = splitName.replace("{" + col + "}", uR.asString(col));
            }

            int hash = u.getKey();
            JavaRDD<Object> split = cachedInput.mapPartitions(it -> {
                List<Object> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Record v = (Record) it.next();

                    if (v.hashCode() == hash) {
                        ret.add(v);
                    }
                }

                return ret.iterator();
            });

            output.put(splitName, new DataStream(input.streamType, split, input.accessor.attributes()));
        }

        return output;
    }
}
