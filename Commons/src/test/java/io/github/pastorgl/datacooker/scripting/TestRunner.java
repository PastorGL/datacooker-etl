/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class TestRunner implements AutoCloseable {
    private final JavaSparkContext context;
    private final ScriptHolder script;

    public TestRunner(String path) {
        this(path, null);
    }

    public TestRunner(String path, Map overrides) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Test Runner")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
                .setMaster("local[*]")
                .set("spark.network.timeout", "10000")
                .set("spark.ui.enabled", "false");
        context = new JavaSparkContext(sparkConf);
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            script = new ScriptHolder(IOUtils.toString(input, StandardCharsets.UTF_8), overrides != null ? overrides : Collections.emptyMap());
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    public Map<String, JavaRDDLike> go() {
        try {
            TDL4Interpreter tdl4 = new TDL4Interpreter(script);
            tdl4.initialize(new TestDataContext(context));
            tdl4.interpret();

            return tdl4.result().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        context.stop();
    }
}
