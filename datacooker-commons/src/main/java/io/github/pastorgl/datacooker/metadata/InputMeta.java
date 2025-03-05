/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

public class InputMeta implements InputOutputMeta {
    public final String descr;

    public final StreamTypes type;

    public final boolean optional;

    InputMeta(StreamTypes type, String descr) {
        this.descr = descr;
        this.type = type;
        this.optional = false;
    }

    @JsonCreator
    InputMeta(StreamTypes type, String descr, boolean optional) {
        this.descr = descr;

        this.type = type;
        this.optional = optional;
    }
}
