/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import java.util.Set;

public abstract class OptionsProvider {
    public abstract Set<String> getAll();

    public abstract Object get(String name);
}
