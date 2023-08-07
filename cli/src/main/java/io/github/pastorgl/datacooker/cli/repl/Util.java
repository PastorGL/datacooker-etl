/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.storage.Adapters;

public class Util {
    static public void populateEntities() {
        RegisteredPackages.REGISTERED_PACKAGES.size();
        Adapters.INPUTS.size();
        Transforms.TRANSFORMS.size();
        Operations.OPERATIONS.size();
        Adapters.OUTPUTS.size();
    }
}
