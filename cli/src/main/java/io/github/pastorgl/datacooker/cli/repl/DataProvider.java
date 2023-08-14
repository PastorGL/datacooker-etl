/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import java.util.Set;
import java.util.stream.Stream;

public abstract class DataProvider {
    public abstract Set<String> getAll();

    public abstract boolean has(String dsName);

    public abstract StreamInfo get(String dsName);

    public abstract Stream<String> sample(String dsName, int limit);

    public abstract void renounce(String dsName);
}
