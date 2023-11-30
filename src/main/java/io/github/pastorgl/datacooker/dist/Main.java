/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.dist;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.*;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);
    static final String DIST_NAME = "Data Cooker Dist";

    public static void main(String[] args) {
        Configuration configBuilder = new Configuration();

        JavaSparkContext context = null;
        try {
            configBuilder.setCommandLine(args, DIST_NAME);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(DIST_NAME)
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean local = configBuilder.hasOption("local");
            if (local) {
                String cores = "*";
                if (configBuilder.hasOption("localCores")) {
                    cores = configBuilder.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000");

                if (configBuilder.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", configBuilder.getOptionValue("driverMemory"));
                }
                sparkConf.set("spark.ui.enabled", String.valueOf(configBuilder.hasOption("sparkUI")));
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            String configPath = configBuilder.getOptionValue("c");
            if (configPath == null) {
                throw new Exception("Configuration file path not specified");
            }

            Path sourcePath = new Path(configPath);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = configPath.lastIndexOf('/');
            configPath = (lastSlash < 0) ? configPath : configPath.substring(0, lastSlash);

            Reader sourceReader = new StringReader(
                    context.wholeTextFiles(configPath)
                            .filter(t -> t._1.equals(qualifiedPath))
                            .map(Tuple2::_2)
                            .first()
            );
            configBuilder.read(sourceReader);

            Collection<String> directions = configBuilder.directions();
            if (directions.isEmpty()) {
                throw new Exception("Configuration file contains no copy directions");
            }

            String distDirection = configBuilder.getOptionValue("d");
            if ((distDirection != null) && !directions.contains(distDirection)) {
                throw new Exception("Configuration file contains no specified copy direction '" + distDirection + "'");
            }
            if ((directions.size() == 1) && (distDirection == null)) {
                distDirection = directions.stream().findFirst().get();
            }
            if ((directions.size() > 1) && (distDirection == null)) {
                throw new Exception("Configuration file contains several copy directions, but none was requested");
            }

            String tmp = configBuilder.getOptionValue("t");
            if (null == tmp) {
                tmp = local ? System.getProperty("java.io.tmpdir") : "hdfs:///tmp";
            }
            Map<String, Object> globalParams = Collections.singletonMap("tmp", tmp);

            Configuration.DistTask[] direction = configBuilder.getDirection(distDirection);
            for (int i = 0; i < direction.length; i++) {
                Configuration.DistTask distTask = direction[i];

                String from = distTask.source.adapter;
                String to = distTask.dest.adapter;

                InputAdapterInfo inputAdapter = Adapters.INPUTS.get(from);
                if (inputAdapter == null) {
                    throw new InvalidConfigurationException("Adapter named '" + from + "' not found");
                }

                Map<String, Object> params = new HashMap<>(globalParams);
                params.putAll(distTask.source.params);
                InputAdapter ia = inputAdapter.configurable.getDeclaredConstructor().newInstance();
                io.github.pastorgl.datacooker.config.Configuration config = new io.github.pastorgl.datacooker.config.Configuration(ia.meta.definitions, "Input " + ia.meta.verb, params);
                ia.initialize(context, config, distTask.source.path);

                String sourceSubName = (distTask.source.subName != null) ? distTask.source.subName : (distDirection + "#" + i);
                ListOrderedMap<String, DataStream> rdds = ia.load(sourceSubName, distTask.source.partNum, Partitioning.HASHCODE);

                for (Map.Entry<String, DataStream> ds : rdds.entrySet()) {
                    OutputAdapterInfo outputAdapter = Adapters.OUTPUTS.get(to);
                    if (outputAdapter == null) {
                        throw new InvalidConfigurationException("Adapter named '" + to + "' not found");
                    }

                    OutputAdapter oa = outputAdapter.configurable.getDeclaredConstructor().newInstance();
                    HashMap<String, Object> outParams = new HashMap<>(globalParams);
                    outParams.putAll(distTask.dest.params);
                    oa.initialize(context, new io.github.pastorgl.datacooker.config.Configuration(oa.meta.definitions, "Output " + oa.meta.verb, outParams), distTask.dest.path);

                    String subName = (distTask.dest.subName != null) ? distTask.dest.subName : "";
                    oa.save(subName, ds.getValue());
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                configBuilder.printHelp(DIST_NAME);
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
