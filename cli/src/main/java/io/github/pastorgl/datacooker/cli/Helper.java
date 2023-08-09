/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.StringReader;
import java.util.*;

public class Helper {
    static public void populateEntities() {
        RegisteredPackages.REGISTERED_PACKAGES.size();
        Adapters.INPUTS.size();
        Transforms.TRANSFORMS.size();
        Operations.OPERATIONS.size();
        Adapters.OUTPUTS.size();
    }

    public static String loadScript(String sourceFile, JavaSparkContext context) {
        try {
            Path sourcePath = new Path(sourceFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = sourceFile.lastIndexOf('/');
            sourceFile = (lastSlash < 0) ? sourceFile : sourceFile.substring(0, lastSlash);

            return context.wholeTextFiles(sourceFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first();
        } catch (Exception e) {
            throw new RuntimeException("Error while reading TDL4 script file");
        }
    }

    public static VariablesContext loadVariables(Configuration config, JavaSparkContext context) throws Exception {
        StringBuilder variablesSource = new StringBuilder();
        if (config.hasOption("v")) {
            String variablesFile = config.getOptionValue("v");

            Path sourcePath = new Path(variablesFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = variablesFile.lastIndexOf('/');
            variablesFile = (lastSlash < 0) ? variablesFile : variablesFile.substring(0, lastSlash);

            variablesSource.append(context.wholeTextFiles(variablesFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first());
        }
        if (config.hasOption("V")) {
            variablesSource.append("\n");
            variablesSource.append(new String(Base64.getDecoder().decode(config.getOptionValue("V"))));
        }

        Properties properties = new Properties();
        if (variablesSource.length() > 0) {
            properties.load(new StringReader(variablesSource.toString()));
        }

        Map<String, Object> variables = new HashMap<>();
        for (Map.Entry e : properties.entrySet()) {
            String key = String.valueOf(e.getKey());
            Object v = e.getValue();
            String value = String.valueOf(v);

            int last = value.length() - 1;
            if ((value.indexOf('[') == 0) && (value.lastIndexOf(']') == last)) {
                value = value.substring(1, last);

                if (value.contains("'")) {
                    boolean inString = false;
                    List<String> strings = new ArrayList<>();
                    StringBuilder cur = null;
                    for (int i = 0, len = value.length(); i < len; i++) {
                        char c = value.charAt(i);
                        if (inString) {
                            if (c != '\'') {
                                cur.append(c);
                            } else { // c == '
                                if ((i + 1) < len) {
                                    if (value.charAt(i + 1) != '\'') {
                                        inString = false;
                                        strings.add(cur.toString());
                                    } else {
                                        cur.append("'");
                                        i++;
                                    }
                                } else {
                                    strings.add(cur.toString());
                                }
                            }
                        } else {
                            if (c == '\'') {
                                inString = true;
                                cur = new StringBuilder();
                            }
                        }
                    }

                    v = strings.toArray();
                } else {
                    String[] vv = value.split(",");
                    v = Arrays.stream(vv).map(vvv -> Double.parseDouble(vvv.trim())).toArray();
                }
            } else if ((value.indexOf('\'') == 0) && (value.lastIndexOf('\'') == last)) {
                v = value.substring(1, last);
            }
            variables.put(key, v);
        }

        VariablesContext variablesContext = new VariablesContext();
        variablesContext.putAll(variables);
        return variablesContext;
    }
}
