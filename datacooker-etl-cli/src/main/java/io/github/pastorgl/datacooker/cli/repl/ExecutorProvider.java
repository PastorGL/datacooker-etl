/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.scripting.Param;
import io.github.pastorgl.datacooker.scripting.TDLErrorListener;

import java.util.List;
import java.util.Map;

public abstract class ExecutorProvider {
    public abstract Object interpretExpr(String expr);

    public abstract String readDirect(String path);
    public abstract String read(String pathExpr);

    public abstract void write(String pathExpr, String recording);

    public abstract void interpret(String script);

    public abstract TDLErrorListener parse(String script);

    public abstract List<String> getAllProcedures();
    public abstract Map<String, Param> getProcedure(String name);
}
