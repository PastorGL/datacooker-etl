/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface Accessor<T> extends Serializable {
    Map<String, List<String>> attributes();

    List<String> attributes(String category);

    void set(T obj, String attr, Object value);

    AttrGetter getter(T obj);
}
