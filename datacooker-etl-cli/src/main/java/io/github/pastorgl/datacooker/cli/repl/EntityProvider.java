/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.metadata.*;

import java.util.Set;

public abstract class EntityProvider {
    public abstract Set<String> getAllPackages();
    public abstract Set<String> getAllTransforms();
    public abstract Set<String> getAllOperations();
    public abstract Set<String> getAllInputs();
    public abstract Set<String> getAllOutputs();
    public abstract Set<String> getAllOperators();
    public abstract Set<String> getAllFunctions();

    public abstract boolean hasPackage(String name);
    public abstract boolean hasTransform(String name);
    public abstract boolean hasOperation(String name);
    public abstract boolean hasInput(String name);
    public abstract boolean hasOutput(String name);
    public abstract boolean hasOperator(String symbol);
    public abstract boolean hasFunction(String symbol);

    public abstract PackageInfo getPackage(String name);
    public abstract PluggableMeta getTransform(String name);
    public abstract PluggableMeta getOperation(String name);
    public abstract PluggableMeta getInput(String name);
    public abstract PluggableMeta getOutput(String name);
    public abstract OperatorInfo getOperator(String symbol);
    public abstract FunctionInfo getFunction(String symbol);
}
