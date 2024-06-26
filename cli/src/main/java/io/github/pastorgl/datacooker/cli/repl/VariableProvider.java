/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.scripting.VariableInfo;

import java.util.Set;

public abstract class VariableProvider {
    public abstract Set<String> getAll();

    public abstract VariableInfo getVar(String name);
}
