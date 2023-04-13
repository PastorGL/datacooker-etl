/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.SingletonMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class StructuredAccessor implements Accessor<Structured> {
    final HashMap<String, Integer> columns;

    public StructuredAccessor(Map<String, List<String>> propNames) {
        this.columns = new HashMap<>();
        int[] n = {0};
        if (!propNames.isEmpty()) {
            propNames.get(OBJLVL_VALUE).forEach(e -> this.columns.put(e, n[0]++));
        }
    }

    public List<String> attributes(String category) {
        return new ArrayList<>(columns.keySet());
    }

    @Override
    public Map<String, List<String>> attributes() {
        return new SingletonMap<>(OBJLVL_VALUE, new ArrayList<>(columns.keySet()));
    }

    @Override
    public void set(Structured rec, String column, Object value) {
        if (!columns.containsKey(column)) {
            columns.put(column, columns.size());
        }
        rec.put(column, value);
    }

    @Override
    public AttrGetter getter(Structured rec) {
        return rec::asIs;
    }
}
