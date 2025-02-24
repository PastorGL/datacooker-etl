/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamConverter;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import org.apache.commons.collections4.map.ListOrderedMap;

class TransformCaller extends Operation {
    private Configuration params;

    public TransformCaller() {
        super();
    }

    @Override
    public OperationMeta initMeta() {
        return null;
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        this.params = params;
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() throws InvalidConfigurationException {
        try {
            StreamConverter converter = Transforms.TRANSFORMS.get(params.verb).configurable.getDeclaredConstructor().newInstance().converter();

            ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
            for (int i = 0, len = inputStreams.size(); i < len; i++) {
                outputs.put(outputStreams.get(i), converter.apply(inputStreams.getValue(i), null, params));
            }

            return outputs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
