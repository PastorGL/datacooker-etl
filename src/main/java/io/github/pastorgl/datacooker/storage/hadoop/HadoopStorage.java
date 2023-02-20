/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.DefinitionEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopStorage {
    public static final String SCHEMA_DEFAULT = "schema_default";
    public static final String SCHEMA_FROM_FILE = "schema_from_file";
    public static final String COLUMNS = "columns";
    public static final String DELIMITER = "delimiter";
    public static final String PART_COUNT = "part_count";
    public static final String SUB_DIRS = "split_sub_dirs";
    public static final String CODEC = "codec";

    public static final String PATH_PATTERN = "^([^:]+:/*[^/]+)/(.+)";

    /**
     * Create a list of file groups from a glob pattern.
     *
     * @param inputPath glob pattern(s) to files
     * @return list of groups in form of Tuple3 [ group name, path, regex ]
     * @throws InvalidConfigurationException if glob pattern is incorrect
     */
    public static List<Tuple2<String, String>> srcDestGroup(String inputPath) throws InvalidConfigurationException {
        List<Tuple2<String, String>> ret = new ArrayList<>();

        int curlyLevel = 0;

        List<String> splits = new ArrayList<>();

        StringBuilder current = new StringBuilder();
        for (int i = 0; i < inputPath.length(); i++) {
            char c = inputPath.charAt(i);

            switch (c) {
                case '\\': {
                    current.append(c).append(inputPath.charAt(++i));
                    break;
                }
                case '{': {
                    curlyLevel++;
                    current.append(c);
                    break;
                }
                case '}': {
                    curlyLevel--;
                    current.append(c);
                    break;
                }
                case ',': {
                    if (curlyLevel == 0) {
                        splits.add(current.toString());
                        current = new StringBuilder();
                    } else {
                        current.append(c);
                    }
                    break;
                }
                default: {
                    current.append(c);
                }
            }
        }
        splits.add(current.toString());

        for (String split : splits) {
            Matcher m = Pattern.compile(PATH_PATTERN).matcher(split);
            if (m.matches()) {
                String rootPath = m.group(1);
                String path = m.group(2);

                List<String> transSubs = new ArrayList<>();
                int groupingSub = -1;

                String sub = path;
                int s = 0;

                nextSub:
                while (true) {
                    StringBuilder translatedSub = new StringBuilder();

                    curlyLevel = 0;
                    boolean inSet = false;
                    for (int i = 0; i < sub.length(); i++) {
                        char c = sub.charAt(i);

                        switch (c) {
                            case '/': {
                                if (!inSet && (curlyLevel == 0)) {
                                    transSubs.add(translatedSub.toString());

                                    if (++i != sub.length()) {
                                        s++;

                                        sub = sub.substring(i);
                                        continue nextSub;
                                    } else {
                                        break nextSub;
                                    }
                                } else {
                                    translatedSub.append(c);
                                }
                                break;
                            }
                            case '\\': {
                                translatedSub.append(c);
                                if (++i != sub.length()) {
                                    translatedSub.append(sub.charAt(i));
                                }
                                break;
                            }
                            case '$':
                            case '(':
                            case ')':
                            case '|':
                            case '+': {
                                translatedSub.append('\\').append(c);
                                break;
                            }
                            case '{': {
                                curlyLevel++;
                                translatedSub.append("(?:");
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '}': {
                                if (curlyLevel > 0) {
                                    curlyLevel--;
                                    translatedSub.append(")");
                                } else {
                                    translatedSub.append(c);
                                }
                                break;
                            }
                            case ',': {
                                translatedSub.append((curlyLevel > 0) ? '|' : c);
                                break;
                            }
                            case '?': {
                                translatedSub.append('.');
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '*': {
                                if ((i != (sub.length() - 1)) && (sub.charAt(i + 1) == '*')) {
                                    translatedSub.append(".*");
                                    i++;
                                } else {
                                    translatedSub.append("[^/]*");
                                }
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '[': {
                                inSet = true;
                                translatedSub.append(c);
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '^': {
                                if (inSet) {
                                    translatedSub.append('\\');
                                }
                                translatedSub.append(c);
                                break;
                            }
                            case '!': {
                                translatedSub.append(inSet && ('[' == sub.charAt(i - 1)) ? '^' : '!');
                                break;
                            }
                            case ']': {
                                inSet = false;
                                translatedSub.append(c);
                                break;
                            }
                            default: {
                                translatedSub.append(c);
                            }
                        }
                    }

                    if (inSet || (curlyLevel > 0)) {
                        throw new InvalidConfigurationException("Glob pattern '" + split + "' contains unbalances range [] or braces {} definition");
                    }

                    if (groupingSub < 0) {
                        groupingSub = s;
                    }

                    transSubs.add(translatedSub.toString());

                    break;
                }

                if (s < 1) {
                    groupingSub = 0;
                }

                String groupSub = transSubs.get(groupingSub);

                transSubs.remove(groupingSub);
                transSubs.add(groupingSub, "(" + groupSub + ")");

                rootPath += "/" + StringUtils.join(transSubs.subList(0, groupingSub), '/');
                ret.add(new Tuple2<>(
                        rootPath + groupSub,
                        ".*/" + StringUtils.join(transSubs.subList(groupingSub, transSubs.size()), '/') + ".*"
                ));
            } else {
                throw new InvalidConfigurationException("Glob pattern '" + split + "' must have protocol specification and its first path part must be not a grouping candidate");
            }
        }

        return ret;
    }

    public static String suffix(String name) {
        String suffix = "";

        if (!StringUtils.isEmpty(name)) {
            int dot = name.lastIndexOf('.');
            if (dot > 0) {
                suffix = name.substring(dot + 1);
            }
        }

        return suffix;
    }

    public enum Codec implements DefinitionEnum {
        NONE(null),
        GZ(GzipCodec.class),
        BZ2(BZip2Codec.class),
        SNAPPY(SnappyCodec.class),
        LZ4(Lz4Codec.class),
        ZST(ZStandardCodec.class);

        public final Class<? extends CompressionCodec> codec;

        Codec(Class<? extends CompressionCodec> codec) {
            this.codec = codec;
        }

        @Override
        public String descr() {
            return (codec != null) ? "Compression " + name() : "No compression";
        }

        static public Codec lookup(String suffix) {
            for (Codec codec : Codec.values()) {
                if (codec.name().equalsIgnoreCase(suffix)) {
                    return codec;
                }
            }

            return NONE;
        }
    }
}
