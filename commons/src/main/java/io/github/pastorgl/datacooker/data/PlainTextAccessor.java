/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class PlainTextAccessor implements Accessor {
    private final Map<String, List<String>> ATTRS = Collections.singletonMap(OBJLVL_VALUE, Collections.singletonList("_value"));

    @Override
    public Map<String, List<String>> attributes() {
        return ATTRS;
    }

    @Override
    public List<String> attributes(String objLvl) {
        return Collections.singletonList("_value");
    }
}
