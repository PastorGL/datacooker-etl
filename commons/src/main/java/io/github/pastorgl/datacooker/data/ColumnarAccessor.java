/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class ColumnarAccessor implements Accessor<Columnar> {
    final ListOrderedMap<String, Integer> columns;

    public ColumnarAccessor(Map<String, List<String>> columns) {
        this.columns = new ListOrderedMap<>();
        int[] n = {0};
        columns.get(OBJLVL_VALUE).forEach(e -> this.columns.put(e, n[0]++));
    }

    public List<String> attributes(String category) {
        return columns.keyList();
    }

    @Override
    public Map<String, List<String>> attributes() {
        return new SingletonMap<>(OBJLVL_VALUE, columns.keyList());
    }

    @Override
    public void set(Columnar rec, String column, Object value) {
        if (!columns.containsKey(column)) {
            columns.put(column, columns.size());
        }
        rec.put(column, value);
    }

    @Override
    public AttrGetter getter(Columnar rec) {
        return rec::asIs;
    }
}
