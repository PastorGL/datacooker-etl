/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.data.Record;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public abstract class DataProvider {
    public abstract Set<String> getAll();

    public abstract boolean has(String dsName);

    public abstract DSData get(String dsName);

    public abstract List<Tuple2<Object, Record<?>>> sample(String dsName, int limit);
}
