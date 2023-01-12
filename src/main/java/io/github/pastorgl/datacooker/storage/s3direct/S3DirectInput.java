/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DataHolder;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopInput;
import io.github.pastorgl.datacooker.storage.hadoop.InputFunction;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;
import static io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectInput extends HadoopInput {
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("s3direct", "Input adapter for any S3-compatible storage, based on Hadoop adapter." +
                " Example path: s3d://bucket/key/prefix/glob/pattern/{2020,2021}/{01,02}/*.tsv",

                new DefinitionMetaBuilder()
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
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
    protected void configure() {
        super.configure();

        accessKey = resolver.get(S3D_ACCESS_KEY);
        secretKey = resolver.get(S3D_SECRET_KEY);
        endpoint = resolver.get(S3D_ENDPOINT);
        region = resolver.get(S3D_REGION);

        tmpDir = resolver.get("tmp");
    }

    @Override
    public List<DataHolder> load(String s3path) {
        Matcher m = Pattern.compile(S3DirectStorage.PATH_PATTERN).matcher(s3path);
        m.matches();
        String bucket = m.group(1);
        String keyPrefix = m.group(2);

        AmazonS3 s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(keyPrefix);

        ObjectListing lo;
        List<String> discoveredFiles = new ArrayList<>();
        do {
            lo = s3.listObjects(request);
            discoveredFiles.addAll(lo.getObjectSummaries().stream()
                    .map(S3ObjectSummary::getKey)
                    .collect(Collectors.toList()));
        } while (lo.isTruncated());

        System.out.println("Discovered S3 objects:");
        discoveredFiles.forEach(System.out::println);

        Map<String, List<String>> prefixMap = new HashMap<>();

        if (subs) {
            int prefixLen = keyPrefix.length();
            if (keyPrefix.charAt(prefixLen - 1) == '/') {
                prefixLen--;
            }

            for (String file : discoveredFiles) {
                String ds = "";
                int p = file.substring(prefixLen).indexOf("/");
                if (p != -1) {
                    int l = file.substring(prefixLen).lastIndexOf("/");
                    if (l != p) {
                        ds = file.substring(p + 1, l);
                    }
                }
                prefixMap.compute(ds, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(file);
                    return v;
                });
            }
        } else {
            prefixMap.put("", discoveredFiles);
        }

        List<DataHolder> ret = new ArrayList<>();
        for (Map.Entry<String, List<String>> ds : prefixMap.entrySet()) {
            List<String> files = ds.getValue();

            int groupSize = files.size() / partCount;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> partFiles = new ArrayList<>();
            Lists.partition(files, groupSize).forEach(p -> partFiles.add(new ArrayList<>(p)));

            InputFunction inputFunction = new S3DirectInputFunction(schemaFromFile, schemaDefault, dsColumns, dsDelimiter.charAt(0),
                    endpoint, region, accessKey, secretKey, bucket, tmpDir);

            ret.add(new DataHolder(context.parallelize(partFiles, partFiles.size())
                    .flatMap(inputFunction.build()).repartition(partCount), ds.getKey()));
        }

        return ret;
    }
}
