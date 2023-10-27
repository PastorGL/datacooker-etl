/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.input.HadoopInput;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage.*;

public abstract class S3DirectInput extends HadoopInput {
    protected String accessKey;
    protected String secretKey;
    protected String endpoint;
    protected String region;
    protected String tmpDir;
    protected String bucket;
    protected String keyPrefix;

    @Override
    protected void configure(Configuration params) {
        super.configure(params);

        accessKey = params.get(S3D_ACCESS_KEY);
        secretKey = params.get(S3D_SECRET_KEY);
        endpoint = params.get(S3D_ENDPOINT);
        region = params.get(S3D_REGION);

        tmpDir = params.get("tmp");

        Matcher m = Pattern.compile(S3DirectStorage.PATH_PATTERN).matcher(path);
        m.matches();
        bucket = m.group(1);
        keyPrefix = m.group(2);
    }

    @Override
    public ListOrderedMap<String, DataStream> load(String prefix, int partCount, Partitioning partitioning) {
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

        System.out.println("Discovered " + discoveredFiles.size() + " S3 object(s):");
        discoveredFiles.forEach(System.out::println);

        Map<String, List<String>> subMap = new HashMap<>();

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
                subMap.compute(ds, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(file);
                    return v;
                });
            }
        } else {
            subMap.put("", discoveredFiles);
        }

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
}
