/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;

public abstract class ExecutorProvider {
    public abstract Object interpretExpr(String expr);

    public abstract String read(String pathExpr);

    public abstract void write(String pathExpr, String recording) throws Exception;

    public abstract void interpret(String script);

    public abstract TDL4ErrorListener parse(String script);
}
