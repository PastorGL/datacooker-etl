/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class ColumnarAccessor implements Accessor {
    private final ListOrderedMap<String, Integer> columns = new ListOrderedMap<>();

    public ColumnarAccessor(Map<String, List<String>> columns) {
        int[] n = {0};
        if (columns.containsKey(OBJLVL_VALUE)) {
            columns.get(OBJLVL_VALUE).forEach(e -> this.columns.put(e, n[0]++));
        }
    }

    public List<String> attributes(String objLvl) {
        return columns.keyList();
    }

    @Override
    public Map<String, List<String>> attributes() {
        return new SingletonMap<>(OBJLVL_VALUE, columns.keyList());
    }
}
