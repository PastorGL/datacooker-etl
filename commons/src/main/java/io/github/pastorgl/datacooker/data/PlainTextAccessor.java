/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class PlainTextAccessor implements Accessor<Text> {
    @Override
    public Map<String, List<String>> attributes() {
        return Collections.singletonMap(OBJLVL_VALUE, Collections.singletonList("_value"));
    }

    @Override
    public List<String> attributes(String category) {
        return Collections.singletonList("_value");
    }

    @Override
    public void set(Text obj, String attr, Object value) {

    }

    @Override
    public AttrGetter getter(Text obj) {
        return null;
    }
}
