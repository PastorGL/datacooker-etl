/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.metadata.ConfigurableInfo;
import io.github.pastorgl.datacooker.metadata.OperationMeta;

public class OperationInfo extends ConfigurableInfo<Operation, OperationMeta> {
    public OperationInfo(Class<Operation> opClass, OperationMeta meta) {
        super(opClass, meta);
    }
}
