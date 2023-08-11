/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.Options;

import java.util.Arrays;

public class OptionsInfo {
    public final String descr;
    public final String def;
    public final String value;

    @JsonCreator
    public OptionsInfo(String descr, String def, String value) {
        this.descr = descr;
        this.def = def;
        this.value = value;
    }

    public OptionsInfo(Options options, Object value) {
        this.descr = options.descr();
        this.def = options.def();

        if (value == null) {
            this.value = null;
        } else if (value.getClass().isArray()) {
            this.value = Arrays.toString((Object[]) value);
        } else {
            this.value = value.toString();
        }
    }
}
