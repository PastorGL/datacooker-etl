/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public abstract class Function implements Operator {
    public Function() {
    }

    public static abstract class Unary extends Function {
        @Override
        public int ariness() {
            return 1;
        }
    }

    public static abstract class Binary extends Function {
        @Override
        public int ariness() {
            return 2;
        }
    }
}
