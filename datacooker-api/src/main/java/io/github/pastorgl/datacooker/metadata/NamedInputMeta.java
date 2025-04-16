/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.HashMap;
import java.util.Map;

public class NamedInputMeta implements InputOutputMeta {
    public final Map<String, InputMeta> streams;

    NamedInputMeta() {
        this.streams = new HashMap<>();
    }

    @JsonCreator
    public NamedInputMeta(Map<String, InputMeta> streams) {
        this.streams = streams;
    }
}
