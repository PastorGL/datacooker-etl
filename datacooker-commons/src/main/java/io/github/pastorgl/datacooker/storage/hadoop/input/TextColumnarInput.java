/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.TextColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.Constants.UNDERSCORE;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;

@SuppressWarnings("unused")
public class TextColumnarInput extends HadoopInput {
    public static final String SCHEMA_FROM_FILE = "schema_from_file";
    static final String VERB = "textColumnar";

    protected String dsDelimiter;
    protected boolean schemaFromFile;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports delimited text, optionally compressed. Depending of file structure it may be splittable or not")
                .inputAdapter(new String[]{"hdfs:///path/to/input/with/glob/**/*.tsv", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.bz2"}, true)
                .reqObjLvls(VALUE)
                .output(StreamType.COLUMNAR, "Columnar DS")
                .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                " Files become not splittable in that case",
                        Boolean.class, false, "By default, don't try to get schema from file")
                .def(DELIMITER, "Column delimiter for delimited text",
                        String.class, "\t", "By default, tabulation character")
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        dsDelimiter = params.get(DELIMITER);

        schemaFromFile = params.get(SCHEMA_FROM_FILE);
    }

    @Override
    protected DataStream callForFiles(String name, List<List<String>> partNum) {
        JavaPairRDD<Object, DataRecord<?>> rdd;

        String[] dsColumns = (requestedColumns.get(VALUE) == null) ? null : requestedColumns.get(VALUE).toArray(new String[0]);

        if (schemaFromFile) {
            InputFunction inputFunction = new TextColumnarInputFunction(dsColumns, dsDelimiter.charAt(0), context.hadoopConfiguration(), partitioning);

            rdd = context.parallelize(partNum, partNum.size())
                    .flatMapToPair(inputFunction.build())
                    .repartition(partCount);
        } else {
            if (dsColumns == null) {
                throw new InvalidConfigurationException("'Schema from file' flag is not set and explicit columns list is empty");
            }

            List<Integer> columns = new ArrayList<>();
            List<String> cols = new ArrayList<>();
            int j = 0;
            for (String column : dsColumns) {
                if (!UNDERSCORE.equals(column)) {
                    columns.add(j);
                    cols.add(column);
                }
                j++;
            }
            final int[] _order = new int[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                _order[i] = columns.get(i);
            }

            final char _delimiter = dsDelimiter.charAt(0);
            final String _source = partNum.stream().map(l -> String.join(",", l)).collect(Collectors.joining(","));
            final Partitioning _partitioning = partitioning;
            rdd = context.textFile(_source, partCount)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();
                        Random random = new Random();
                        while (it.hasNext()) {
                            String[] ll = parser.parseLine(it.next());

                            String[] acc = new String[_order.length];
                            for (int i = 0; i < _order.length; i++) {
                                int l = _order[i];
                                acc[i] = ll[l];
                            }
                            DataRecord<?> rec = new Columnar(cols, acc);

                            Object key = switch (_partitioning) {
                                case HASHCODE -> rec.hashCode();
                                case RANDOM -> random.nextInt();
                                case SOURCE -> _source.hashCode();
                            };
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
        }

        return new DataStreamBuilder(name, Collections.singletonMap(VALUE, attrs))
                .created(VERB, path, StreamType.Columnar, partitioning.toString())
                .build(rdd);
    }
}
