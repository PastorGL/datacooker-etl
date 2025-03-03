/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.ObjLvl;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class StreamInfo {
    public final Map<ObjLvl, List<String>> attrs;
    public final String keyExpression;
    public final String sl;
    public final String streamType;
    public final int numPartitions;
    public final int usages;

    @JsonCreator
    public StreamInfo(Map<ObjLvl, List<String>> attrs, String keyExpression, String sl, String streamType, int numPartitions, int usages) {
        this.attrs = attrs;
        this.keyExpression = keyExpression;
        this.sl = sl;
        this.streamType = streamType;
        this.numPartitions = numPartitions;
        this.usages = usages;
    }

    public String describe(Object utDef) {
        StringBuilder sb = new StringBuilder(streamType + ", " + numPartitions + " partition(s)\n");
        for (Map.Entry<ObjLvl, List<String>> cat : attrs.entrySet()) {
            sb.append(StringUtils.capitalize(cat.getKey().name()) + " attributes:\n\t" + String.join(", ", cat.getValue()) + "\n");
        }
        sb.append("Key expression:\n\t" + keyExpression + "\n");
        sb.append(usages + " usage(s) with threshold of " + utDef + ", " + sl + "\n");

        return sb.toString();
    }
}
