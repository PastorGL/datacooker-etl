/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.time.Instant;
import java.util.Date;

public class DateTime {
    public static Date parseTimestamp(Object tsObject) {
        Double timestamp;

        if (tsObject instanceof Number) {
            timestamp = ((Number) tsObject).doubleValue();
        } else {
            String tsText = String.valueOf(tsObject);

            try {
                timestamp = Double.parseDouble(tsText);
            } catch (Exception ignore) {
                return Date.from(Instant.parse(tsText));
            }
        }

        if (timestamp < 100_000_000_000.D) {
            timestamp *= 1000.D;
        }

        return new Date(timestamp.longValue());
    }
}
