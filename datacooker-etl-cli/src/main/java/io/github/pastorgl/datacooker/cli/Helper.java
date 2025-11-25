/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.metadata.Pluggables;
import io.github.pastorgl.datacooker.scripting.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Function1;
import scala.Tuple2;

import java.io.StringReader;
import java.net.URL;
import java.util.*;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.github.pastorgl.datacooker.Constants.CWD_VAR;
import static io.github.pastorgl.datacooker.Constants.ENV_VAR_PREFIX;
import static io.github.pastorgl.datacooker.DataCooker.GLOBAL_VARS;
import static io.github.pastorgl.datacooker.cli.Main.LOG;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.pathToGroups;
import static org.burningwave.core.assembler.StaticComponentContainer.Modules;

public class Helper {
    static public void populateEntities() {
        Map<String, Integer> stat = Pluggables.load();
        String[] msg = new String[stat.size() + 1];
        msg[0] = "Pluggables discovered in the Classpath:";
        int[] i = {1};
        stat.forEach((key, value) -> msg[i[0]++] = key + " " + value);
        log(msg);
    }

    static public void log(String[] msg, Object... err) {
        Function1<String, Void> lf = (err.length > 0)
                ? (m) -> {
            LOG.error(m);
            return null;
        }
                : (m) -> {
            LOG.warn(m);
            return null;
        };
        int len = Math.min(Arrays.stream(msg).map(String::length).max(Integer::compareTo).orElse(40), 40);
        lf.apply(StringUtils.repeat("=", len));
        Arrays.stream(msg).forEach(lf::apply);
        lf.apply(StringUtils.repeat("=", len));
    }

    public static String loadScript(String path, JavaSparkContext context) {
        StringBuilder scriptSource = new StringBuilder();
        try {
            List<Tuple2<String, String>> splits = pathToGroups(path);

            for (Tuple2<String, String> split : splits) {
                Path srcPath = new Path(split._1);
                Pattern pattern = Pattern.compile(split._2);

                FileSystem srcFS = srcPath.getFileSystem(context.hadoopConfiguration());
                RemoteIterator<LocatedFileStatus> files = srcFS.listFiles(srcPath, true);
                while (files.hasNext()) {
                    String srcFile = files.next().getPath().toString();

                    Matcher m = pattern.matcher(srcFile);
                    if (m.matches()) {
                        scriptSource.append(context.wholeTextFiles(srcFile).values().reduce(String::concat));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while reading TDL script file");
        }
        return scriptSource.toString();
    }

    public static void populateVariables(Configuration config, JavaSparkContext context) throws Exception {
        StringBuilder variablesSource = new StringBuilder();
        if (config.hasOption("v")) {
            String path = config.getOptionValue("v");

            List<Tuple2<String, String>> splits = pathToGroups(path);

            for (Tuple2<String, String> split : splits) {
                Path srcPath = new Path(split._1);
                Pattern pattern = Pattern.compile(split._2);

                FileSystem srcFS = srcPath.getFileSystem(context.hadoopConfiguration());
                RemoteIterator<LocatedFileStatus> files = srcFS.listFiles(srcPath, true);
                while (files.hasNext()) {
                    String srcFile = files.next().getPath().toString();

                    Matcher m = pattern.matcher(srcFile);
                    if (m.matches()) {
                        variablesSource.append(context.wholeTextFiles(srcFile).values().reduce(String::concat));
                    }
                }
            }
        }
        if (config.hasOption("V")) {
            variablesSource.append("\n");
            variablesSource.append(new String(Base64.getDecoder().decode(config.getOptionValue("V"))));
        }

        Properties properties = new Properties();
        if (!variablesSource.isEmpty()) {
            properties.load(new StringReader(variablesSource.toString()));
        }

        Map<String, Object> variables = new HashMap<>();
        for (Map.Entry<?, ?> e : properties.entrySet()) {
            String key = String.valueOf(e.getKey());
            Object v = e.getValue();
            String value = String.valueOf(v).trim();

            int last = value.length() - 1;
            if ((value.indexOf('[') == 0) && (value.lastIndexOf(']') == last)) {
                value = value.substring(1, last).trim();

                if (value.startsWith("'")) {
                    v = getQuotedStrings(value, '\'');
                } else if (value.startsWith("\"")) {
                    v = getQuotedStrings(value, '"');
                } else {
                    String[] vv = value.split(",");
                    v = Arrays.stream(vv).map(vvv -> Utils.parseNumber(vvv.trim())).toArray(Number[]::new);
                }
            } else if ((value.length() >= 2) && (value.indexOf('\'') == 0) && (value.lastIndexOf('\'') == last)) {
                v = value.substring(1, last);
            } else if ((value.length() >= 2) && (value.indexOf('"') == 0) && (value.lastIndexOf('"') == last)) {
                v = value.substring(1, last);
            }
            variables.put(key, v);
        }

        variables.put(CWD_VAR, java.nio.file.Path.of("").toAbsolutePath().toString());
        System.getenv().forEach((key, value) -> variables.put(ENV_VAR_PREFIX + key, value));

        GLOBAL_VARS.putAll(variables);
    }

    private static Object getQuotedStrings(String value, char quote) {
        boolean inString = false;
        List<String> strings = new ArrayList<>();
        StringBuilder cur = null;
        for (int i = 0, len = value.length(); i < len; i++) {
            char c = value.charAt(i);
            if (inString) {
                if (c != quote) {
                    cur.append(c);
                } else {
                    if ((i + 1) < len) {
                        if (value.charAt(i + 1) != quote) {
                            inString = false;
                            strings.add(cur.toString());
                        } else {
                            cur.append(quote);
                            i++;
                        }
                    } else {
                        strings.add(cur.toString());
                    }
                }
            } else {
                if (c == quote) {
                    inString = true;
                    cur = new StringBuilder();
                }
            }
        }

        return strings.toArray(new String[0]);
    }

    public static String getVersion() {
        try {
            URL url = Main.class.getClassLoader().getResource("META-INF/MANIFEST.MF");
            Manifest man = new Manifest(url.openStream());

            return man.getMainAttributes().getValue("Implementation-Version");
        } catch (Exception ignore) {
            return "unknown";
        }
    }

    public static void exportAllToAll() {
        Modules.exportAllToAll();
    }
}
