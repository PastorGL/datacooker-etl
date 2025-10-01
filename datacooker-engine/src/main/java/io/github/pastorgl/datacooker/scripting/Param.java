/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.Serializable;

public class Param implements Serializable {
    public final String comment;
    public final boolean optional;
    public final Object defaults;
    public final String defComment;

    @JsonCreator
    public Param(String comment, boolean optional, Object defaults, String defComment) {
        this.comment = comment;
        this.optional = optional;
        this.defaults = defaults;
        this.defComment = defComment;
    }

    Param(String comment, Object defaults, String defComment) {
        this.comment = comment;
        this.optional = true;
        this.defaults = defaults;
        this.defComment = defComment;
    }

    Param(String comment) {
        this.comment = comment;
        this.optional = false;
        this.defaults = null;
        this.defComment = null;
    }
}
