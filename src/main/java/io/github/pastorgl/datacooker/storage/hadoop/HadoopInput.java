/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DataHolder;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.InputAdapter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

public class HadoopInput extends InputAdapter {
    protected boolean subs;

    protected int partCount;
    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String[] dsColumns;
    protected String dsDelimiter;

    protected int numOfExecutors;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("hadoop", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text, delimited text (CSV/TSV), and Parquet files, optionally compressed. Path examples:" +
                " hdfs:///path/to/input/with/glob/**/*.tsv, file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet," +
                " s3://bucket/path/to/data/group-000??",

                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                        " Ignored for Parquet",
                                Boolean.class, true, "By default, do try")
                        .def(SCHEMA_DEFAULT, "Loose schema for delimited text (just column names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)." +
                                        " Only if " + SCHEMA_FROM_FILE + " is set to false",
                                String[].class, null, "By default, don't set the schema," +
                                        " so the file will be plain text")
                        .def(DELIMITER, "Column delimiter for delimited text",
                                String.class, "\t", "By default, tabulation character")
                        .def(COLUMNS, "Columns to select from the schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .def(PART_COUNT, "Desired number of parts",
                                Integer.class, 1, "By default, one part")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        subs = resolver.get(SUB_DIRS);

        dsDelimiter = resolver.get(DELIMITER);

        schemaFromFile = resolver.get(SCHEMA_FROM_FILE);
        if (!schemaFromFile) {
            schemaDefault = resolver.get(SCHEMA_DEFAULT);
        }

        dsColumns = resolver.get(COLUMNS);

        partCount = Math.max(resolver.get(PART_COUNT), 1);

        int executors = Integer.parseInt(context.getConf().get("spark.executor.instances", "-1"));
        numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);

        if (partCount <= 0) {
            partCount = numOfExecutors;
        }
    }

    @Override
    public List<DataHolder> load(String globPattern) {
        // path, regex
        List<Tuple2<String, String>> splits = HadoopStorage.srcDestGroup(globPattern);

        // files
        List<Tuple2<String, String>> discoveredFiles = context.parallelize(splits, numOfExecutors)
                .flatMap(srcDestGroup -> {
                    List<Tuple2<String, String>> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1);

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._2);

                        while (srcFiles.hasNext()) {
                            String srcFile = srcFiles.next().getPath().toString();

                            Matcher m = pattern.matcher(srcFile);
                            if (m.matches()) {
                                files.add(new Tuple2<>(srcDestGroup._1, srcFile));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while enumerating files to copy: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(13);
                    }

                    return files.iterator();
                })
                .collect();

        System.out.println("Discovered Hadoop FileSystem files:");
        discoveredFiles.stream().map(Tuple2::_2).forEach(System.out::println);

        Map<String, List<String>> prefixMap = new HashMap<>();

        if (subs) {
            for (Tuple2<String, String> file : discoveredFiles) {
                int prefixLen = file._1.length();
                if (file._1.charAt(prefixLen - 1) == '/') {
                    prefixLen--;
                }

                String ds = "";
                int p = file._2.substring(prefixLen).indexOf("/");
                if (p != -1) {
                    int l = file._2.substring(prefixLen).lastIndexOf("/");
                    if (l != p) {
                        ds = file._2.substring(p + 1, l);
                    }
                }
                prefixMap.compute(ds, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(file._2);
                    return v;
                });
            }
        } else {
            prefixMap.put("", discoveredFiles.stream().map(Tuple2::_2).collect(Collectors.toList()));
        }

        List<DataHolder> ret = new ArrayList<>();
        for (Map.Entry<String, List<String>> ds : prefixMap.entrySet()) {
            List<String> files = ds.getValue();

            int groupSize = files.size() / partCount;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> partNum = new ArrayList<>();
            Lists.partition(files, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

            InputFunction inputFunction = new InputFunction(schemaFromFile, schemaDefault, dsColumns, dsDelimiter.charAt(0));
            return Collections.singletonList(new DataHolder(context.parallelize(partNum, partNum.size())
                    .flatMap(inputFunction.build()).repartition(partCount), ds.getKey()));
        }

        return ret;
    }
}
