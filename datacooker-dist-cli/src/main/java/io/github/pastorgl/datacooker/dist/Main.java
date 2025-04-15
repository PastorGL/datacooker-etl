/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.dist;

import io.github.pastorgl.datacooker.config.*;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;
import io.github.pastorgl.datacooker.metadata.Pluggables;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.burningwave.core.assembler.StaticComponentContainer.Modules;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);
    static final String DIST_NAME = "Data Cooker Dist";

    public static void main(String[] args) {
        Modules.exportAllToAll();

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
            for (Configuration.DistTask distTask : direction) {
                String from = distTask.source.verb;
                String to = distTask.dest.verb;

                PluggableInfo iaInfo = Pluggables.INPUTS.get(from);
                if (iaInfo == null) {
                    throw new InvalidConfigurationException("Input Adapter named '" + from + "' not found");
                }
                PluggableInfo oaInfo = Pluggables.OUTPUTS.get(to);
                if (oaInfo == null) {
                    throw new InvalidConfigurationException("Output Adapter named '" + to + "' not found");
                }

                List<PluggableInfo> trInfos = new ArrayList<>();
                if (distTask.transform != null) {
                    for (Configuration.Transform transform : distTask.transform) {
                        String tr = transform.verb;

                        PluggableInfo trInfo = Pluggables.TRANSFORMS.get(tr);
                        if (trInfo == null) {
                            throw new InvalidConfigurationException("Transform named '" + tr + "' not found");
                        }
                        trInfos.add(trInfo);
                    }
                }

                Map<String, Object> iaParams = new HashMap<>(globalParams);
                iaParams.putAll(distTask.source.params);

                Pluggable ia = iaInfo.newInstance();
                ia.configure(new io.github.pastorgl.datacooker.config.Configuration(iaInfo.meta.definitions, "Input " + iaInfo.meta.verb, iaParams));
                ia.initialize(
                        new PathInput(context, distTask.source.path, distTask.source.wildcard, distTask.source.partNum, Partitioning.get(distTask.source.partitioning)),
                        new Output(distTask.name, distTask.source.columns.entrySet().stream().collect(Collectors.toMap(k -> ObjLvl.get(k.getKey()), Map.Entry::getValue)))
                );
                ia.execute();
                Map<String, DataStream> streams = ia.result();

                if (!trInfos.isEmpty()) {
                    Pluggable[] tr = new Pluggable[trInfos.size()];
                    for (int i = 0; i < tr.length; i++) {
                        PluggableInfo trInfo = trInfos.get(i);

                        Map<String, Object> trParams = new HashMap<>(globalParams);
                        trParams.putAll(distTask.transform[i].params);

                        tr[i] = trInfo.newInstance();
                        tr[i].configure(new io.github.pastorgl.datacooker.config.Configuration(trInfo.meta.definitions, "Transform " + trInfo.meta.verb, trParams));
                    }

                    Map<String, DataStream> transformed = new HashMap<>();
                    for (Map.Entry<String, DataStream> ds : streams.entrySet()) {
                        String dsName = ds.getKey();
                        DataStream dataStream = ds.getValue();
                        for (int i = 0; i < tr.length; i++) {
                            tr[i].initialize(
                                    new Input(dataStream),
                                    new Output(dsName, distTask.transform[i].columns.entrySet().stream().collect(Collectors.toMap(k -> ObjLvl.get(k.getKey()), Map.Entry::getValue)))
                            );
                            tr[i].execute();
                            Map<String, DataStream> result = tr[i].result();
                            dataStream = result.get(dsName);
                        }
                        transformed.put(dsName, dataStream);
                    }
                    streams = transformed;
                }

                HashMap<String, Object> oaParams = new HashMap<>(globalParams);
                oaParams.putAll(distTask.dest.params);

                Pluggable oa = oaInfo.newInstance();
                oa.configure(new io.github.pastorgl.datacooker.config.Configuration(oaInfo.meta.definitions, "Output " + oaInfo.meta.verb, oaParams));

                for (DataStream ds : streams.values()) {
                    oa.initialize(
                            new Input(ds),
                            new PathOutput(context, distTask.dest.path, distTask.dest.columns.entrySet().stream().collect(Collectors.toMap(k -> ObjLvl.get(k.getKey()), Map.Entry::getValue)))
                    );
                    oa.execute();
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
