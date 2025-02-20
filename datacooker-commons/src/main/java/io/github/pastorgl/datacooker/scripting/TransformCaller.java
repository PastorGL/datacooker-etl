/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.commons.collections4.map.ListOrderedMap;

public class TransformCaller extends Operation {
    private final Transform tf;
    private Configuration params;

    public TransformCaller(Class<Transform> transform) throws Exception {
        tf = transform.getDeclaredConstructor().newInstance();
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        this.params = params;
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() throws InvalidConfigurationException {
        StreamConverter converter = tf.converter();

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            outputs.put(outputStreams.get(i), converter.apply(inputStreams.getValue(i), null, params));
        }

        return outputs;
    }

    @Override
    public OperationMeta meta() {
        TransformMeta m = tf.meta();

        return new OperationMeta(m.verb, m.descr,
                new PositionalStreamsMetaBuilder()
                        .input(m.from.name(), StreamType.of(m.from))
                        .build(),

                m.definitions,

                new PositionalStreamsMetaBuilder()
                        .generated(m.transformed.stream.generated)
                        .output(m.to.name(), StreamType.of(m.to), StreamOrigin.GENERATED, null)
                        .build()
        );
    }
}
