/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public enum Options implements DefinitionEnum {
    storage_level("Spark storage level for DS with usage count above threshold. As of Spark 3.3 can be on of the" +
            " following: NONE, DISK_ONLY, DISK_ONLY_2, DISK_ONLY_3, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER, MEMORY_ONLY_SER_2," +
            " MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP", "MEMORY_AND_DISK"),
    usage_threshold("Usage count for DS on which Spark storage level is applied to it", "2"),
    log_level("Spark log level. Can be one of: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN", "INFO"),
    batch_verbose("Batch mode verbose flag. If set, interpreter prints info around each TDL operator to stdout", "false");

    private final String descr;
    private final String def;

    Options(String descr, String def) {
        this.descr = descr;
        this.def = def;
    }

    @Override
    public String descr() {
        return descr;
    }

    public String def() {
        return def;
    }

    public static Set<String> getAll() {
        return Arrays.stream(values()).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));
    }
}
