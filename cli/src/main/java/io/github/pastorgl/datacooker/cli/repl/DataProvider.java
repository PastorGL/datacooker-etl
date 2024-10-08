/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.scripting.StreamInfo;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public abstract class DataProvider {
    public abstract Set<String> getAll();

    public abstract boolean has(String dsName);

    public abstract StreamInfo get(String dsName);

    public abstract Stream<String> sample(String dsName, int limit);

    public abstract Stream<String> part(String dsName, int part, int limit);

    public abstract StreamInfo persist(String dsName);

    public abstract void renounce(String dsName);

    public abstract List<StreamLineage> lineage(String dsName);
}
