/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public abstract class Function implements Evaluator {
    public Function() {
    }

    public static abstract class RecordLevel extends Function {
        @Override
        public int arity() {
            return 0;
        }
    }
}
