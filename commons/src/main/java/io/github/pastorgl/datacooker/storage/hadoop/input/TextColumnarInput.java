/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.TextColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.Constants.UNDERSCORE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.COLUMNS;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;

@SuppressWarnings("unused")
public class TextColumnarInput extends HadoopInput {
    public static final String SCHEMA_DEFAULT = "schema_default";
    public static final String SCHEMA_FROM_FILE = "schema_from_file";

    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String[] dsColumns;
    protected String dsDelimiter;

    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("textColumnar", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports delimited text, optionally compressed. Depending of file structure it may be splittable or not",
                new String[]{"hdfs:///path/to/input/with/glob/**/*.tsv", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.bz2"},

                StreamType.Columnar,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                        " Files become not splittable in that case",
                                Boolean.class, false, "By default, don't try to get schema from file")
                        .def(SCHEMA_DEFAULT, "Loose schema for delimited text (just column names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)." +
                                        " Required if " + SCHEMA_FROM_FILE + " is set to false",
                                String[].class, null, "By default, don't set the schema")
                        .def(DELIMITER, "Column delimiter for delimited text",
                                String.class, "\t", "By default, tabulation character")
                        .def(COLUMNS, "Columns to select from the schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        super.configure();

        dsDelimiter = resolver.get(DELIMITER);

        schemaFromFile = resolver.get(SCHEMA_FROM_FILE);
        if (!schemaFromFile) {
            schemaDefault = resolver.get(SCHEMA_DEFAULT);

            if (schemaDefault == null) {
                throw new InvalidConfigurationException("Neither '" + SCHEMA_FROM_FILE + "' is true nor '"
                        + SCHEMA_DEFAULT + "' is specified for Input Adater '" + meta.verb + "'");
            }
        }

        dsColumns = resolver.get(COLUMNS);
    }

    @Override
    protected DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning) {
        JavaPairRDD<Object, Record<?>> rdd;

        if (schemaFromFile) {
            InputFunction inputFunction = new TextColumnarInputFunction(dsColumns, dsDelimiter.charAt(0), partitioning);

            rdd = context.parallelize(partNum, partNum.size())
                    .flatMapToPair(inputFunction.build())
                    .repartition(partCount);
        } else {
            if (dsColumns == null) {
                dsColumns = Arrays.stream(schemaDefault).filter(c -> !UNDERSCORE.equals(c)).toArray(String[]::new);
            }

            Map<String, Integer> schema = new HashMap<>();
            for (int i = 0; i < schemaDefault.length; i++) {
                schema.put(schemaDefault[i], i);
            }

            final int[] order = new int[dsColumns.length];
            for (int i = 0; i < dsColumns.length; i++) {
                order[i] = schema.get(dsColumns[i]);
            }

            final List<String> columns = Arrays.asList(dsColumns);
            final char delimiter = dsDelimiter.charAt(0);
            rdd = context.textFile(partNum.stream().map(l -> String.join(",", l)).collect(Collectors.joining(",")), partCount)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(delimiter).build();
                        Random random = new Random();
                        while (it.hasNext()) {
                            String[] ll = parser.parseLine(it.next());

                            String[] acc = new String[order.length];
                            for (int i = 0; i < order.length; i++) {
                                int l = order[i];
                                acc[i] = ll[l];
                            }
                            Record<?> rec = new Columnar(columns, acc);

                            Object key = (partitioning == Partitioning.RANDOM) ? random.nextInt() : rec.hashCode();
                            ret.add(new Tuple2<>(key, rec));
                        }

                        return ret.iterator();
                    });

            if (partitioning != Partitioning.SOURCE) {
                rdd = rdd.repartition(partCount);
            }
        }

        List<String> attrs = Collections.emptyList();
        if (dsColumns != null) {
            attrs = Arrays.asList(dsColumns);
        } else if (schemaDefault != null) {
            attrs = Arrays.asList(schemaDefault);
        }
        return new DataStream(StreamType.Columnar, rdd, Collections.singletonMap(OBJLVL_VALUE, attrs));
    }
}
