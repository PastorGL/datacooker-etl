/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Record;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class TestRunner implements AutoCloseable {
    private final JavaSparkContext context;
    private final String script;
    private final VariablesContext variables;

    private final TestDataContext dataContext;

    public TestRunner(String path) {
        this(path, null);
    }

    public TestRunner(String path, Map<String, Object> overrides) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Test Runner")
                .setMaster("local[*]")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
                .set("spark.network.timeout", "10000")
                .set("spark.ui.enabled", "false");
// after 3.5.0  .set("spark.log.level", "WARN");
        context = new JavaSparkContext(sparkConf);
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        dataContext = new TestDataContext(context);

        System.out.println("======================================");
        System.out.println("Script path: " + path);

        try (InputStream input = getClass().getResourceAsStream(path)) {
            script = IOUtils.toString(input, StandardCharsets.UTF_8);
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
        variables = new VariablesContext();
        if (overrides != null) {
            variables.putAll(overrides);
        }
    }

    public Map<String, JavaPairRDD<Object, Record<?>>> go() {
        try {
            OptionsContext options = new OptionsContext();
            options.put(Options.batch_verbose.name(), Boolean.TRUE.toString());
            options.put(Options.log_level.name(), "WARN");

            TDL4ErrorListener errorListener = new TDL4ErrorListener();
            TDL4Interpreter tdl4 = new TDL4Interpreter(script, variables, options, errorListener);
            if (errorListener.errorCount > 0) {
                throw new InvalidConfigurationException(errorListener.errorCount + " error(s). First error is '" + errorListener.messages.get(0)
                        + "' @ " + errorListener.lines.get(0) + ":" + errorListener.positions.get(0));
            }

            tdl4.interpret(dataContext);

            return dataContext.result().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().rdd));
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        context.stop();
        dataContext.deleteTempDirs();
    }
}
