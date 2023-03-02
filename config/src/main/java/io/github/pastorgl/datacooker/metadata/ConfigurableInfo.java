/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public class ConfigurableInfo<CA extends Configurable<M>, M extends ConfigurableMeta> {
    public final Class<CA> configurable;
    public final M meta;

    public ConfigurableInfo(Class<CA> configurable, M meta) {
        this.configurable = configurable;
        this.meta = meta;
    }
}
