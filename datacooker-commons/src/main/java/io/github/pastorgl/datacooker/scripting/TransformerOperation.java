/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import org.apache.commons.collections4.map.ListOrderedMap;

public abstract class TransformerOperation extends Operation {
    @Override
    public ListOrderedMap<String, DataStream> execute() throws InvalidConfigurationException {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            outputs.put(outputStreams.get(i), transformer().apply(input, outputStreams.get(i)));
        }

        return outputs;
    }

    abstract protected StreamTransformer transformer();
}
