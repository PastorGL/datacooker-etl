/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = NamedStreamsMeta.class, name = "named"),
        @JsonSubTypes.Type(value = AnonymousStreamMeta.class, name = "anonymous"),
        @JsonSubTypes.Type(value = TransformedStreamMeta.class, name = "transformed")
})
public class DataStreamsMeta implements Serializable {
    public final boolean anonymous;

    protected DataStreamsMeta(boolean anonymous) {
        this.anonymous = anonymous;
    }
}
