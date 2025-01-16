/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import com.google.common.collect.Lists;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.scripting.Utils;
import io.github.pastorgl.datacooker.storage.InputAdapter;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.pathToGroups;

public abstract class HadoopInput extends InputAdapter {
    public static final String SUB_DIRS = "split_sub_dirs";

    protected boolean subs;
    protected int numOfExecutors;

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        subs = params.get(SUB_DIRS);

        int executors = Utils.parseNumber(context.getConf().get("spark.executor.instances", "-1")).intValue();
        numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);
    }

    @Override
    public ListOrderedMap<String, DataStream> load(String prefix, int partCount, Partitioning partitioning) {
        if (partCount <= 0) {
            partCount = numOfExecutors;
        }

        ListOrderedMap<String, List<String>> subMap = new ListOrderedMap<>();

        org.apache.hadoop.conf.Configuration hadoopConf = context.hadoopConfiguration();
        if (subs) {
            Path srcPath = new Path(path);

            try {
                FileSystem srcFS = srcPath.getFileSystem(hadoopConf);

                int pathLen = srcFS.getFileStatus(srcPath).getPath().toString().length();
                RemoteIterator<LocatedFileStatus> files = srcFS.listFiles(srcPath, true);
                while (files.hasNext()) {
                    String file = files.next().getPath().toString();
                    String sub = file.substring(pathLen + 1);

                    int subIndex = sub.indexOf("/");
                    if (subIndex > 0) {
                        sub = sub.substring(0, subIndex);

                        subMap.compute(sub, (k, v) -> {
                            if (v == null) {
                                v = new ArrayList<>();
                            }
                            v.add(file);
                            return v;
                        });
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception while enumerating sub dirs in " + path, e);
            }

            if (subMap.isEmpty()) {
                throw new RuntimeException("No sub dirs found in " + path);
            }
        } else {
            // path, regex
            List<Tuple2<String, String>> splits = pathToGroups(path);

            String confXml;
            try {
                StringWriter sw = new StringWriter();
                hadoopConf.writeXml(sw);
                confXml = sw.toString();
            } catch (IOException ignored) {
                confXml = "";
            }

            String _confXml = confXml;
            // files
            List<String> discoveredFiles = context.parallelize(splits, numOfExecutors)
                    .flatMap(srcDestGroup -> {
                        List<String> ret = new ArrayList<>();
                        try {
                            Path srcPath = new Path(srcDestGroup._1);
                            Pattern pattern = Pattern.compile(srcDestGroup._2);

                            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                            conf.addResource(new ByteArrayInputStream(_confXml.getBytes()));

                            FileSystem srcFS = srcPath.getFileSystem(conf);
                            RemoteIterator<LocatedFileStatus> files = srcFS.listFiles(srcPath, true);
                            while (files.hasNext()) {
                                String srcFile = files.next().getPath().toString();

                                Matcher m = pattern.matcher(srcFile);
                                if (m.matches()) {
                                    ret.add(srcFile);
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Exception while enumerating files in " + srcDestGroup, e);
                        }

                        return ret.iterator();
                    })
                    .collect();

            subMap.put("", discoveredFiles);
        }

        subMap.forEach((key, discoveredFiles) -> {
            if (!key.isEmpty()) {
                System.out.println("Sub dir " + key);
            }
            System.out.println("Discovered " + discoveredFiles.size() + " Hadoop FileSystem file(s):");
            discoveredFiles.forEach(System.out::println);
        });

        ListOrderedMap<String, DataStream> ret = new ListOrderedMap<>();
        for (Map.Entry<String, List<String>> ds : subMap.entrySet()) {
            List<String> files = ds.getValue();

            int groupSize = files.size() / partCount;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> partNum = new ArrayList<>();
            Lists.partition(files, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

            String sub = ds.getKey();
            String name = sub.isEmpty() ? prefix : prefix + "/" + sub;
            ret.put(name, callForFiles(name, partCount, partNum, partitioning));
        }

        return ret;
    }

    protected abstract DataStream callForFiles(String name, int partCount, List<List<String>> partNum, Partitioning partitioning);
}
