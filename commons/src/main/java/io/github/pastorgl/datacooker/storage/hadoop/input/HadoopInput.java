/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import com.google.common.collect.Lists;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.InputAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.srcDestGroup;

public abstract class HadoopInput extends InputAdapter {
    public static final String SUB_DIRS = "split_sub_dirs";

    protected boolean subs;
    protected int numOfExecutors;

    @Override
    protected void configure() throws InvalidConfigurationException {
        subs = resolver.get(SUB_DIRS);

        int executors = Integer.parseInt(context.getConf().get("spark.executor.instances", "-1"));
        numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);
    }

    @Override
    public Map<String, DataStream> load(int partCount, Partitioning partitioning) {
        if (partCount <= 0) {
            partCount = numOfExecutors;
        }

        // path, regex
        List<Tuple2<String, String>> splits = srcDestGroup(path);

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
                        throw new RuntimeException("Exception while enumerating files to copy", e);
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

        Map<String, DataStream> ret = new HashMap<>();
        for (Map.Entry<String, List<String>> ds : prefixMap.entrySet()) {
            List<String> files = ds.getValue();

            int groupSize = files.size() / partCount;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> partNum = new ArrayList<>();
            Lists.partition(files, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

            ret.put(ds.getKey(), callForFiles(partCount, partNum, partitioning));
        }

        return ret;
    }

    protected abstract DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning);
}
