/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.util.Deque;

public abstract class Operator implements Evaluator {
    public abstract int prio();

    public boolean rightAssoc() {
        return false;
    }

    protected boolean handleNull() {
        return false;
    }

    protected abstract Object op0(Deque<Object> args);

    public Operator() {
    }

    @Override
    public Object call(Deque<Object> args) {
        if (!handleNull()) {
            for (Object a : args) {
                if (a == null) {
                    return null;
                }
            }
        }

        return op0(args);
    }
}
