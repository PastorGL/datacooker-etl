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
        @JsonSubTypes.Type(value = NamedInputMeta.class, name = "namedInput"),
        @JsonSubTypes.Type(value = NamedOutputMeta.class, name = "namedOutput"),
        @JsonSubTypes.Type(value = InputMeta.class, name = "anonInput"),
        @JsonSubTypes.Type(value = OutputMeta.class, name = "anonOutput"),
        @JsonSubTypes.Type(value = PathExamplesMeta.class, name = "pathExamples")
})
public interface InputOutputMeta extends Serializable {
}
