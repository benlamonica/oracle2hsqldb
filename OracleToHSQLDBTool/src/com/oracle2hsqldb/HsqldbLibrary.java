/*
 * Schemamule, a library for automating database schema tasks
 * Copyright (C) 2006, Moses M. Hohman and Rhett Sutphin
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St., 5th Floor, Boston, MA  02110-1301

 * To contact the authors, send email to:
 * { mmhohman OR rsutphin } AT sourceforge DOT net
 */

package com.oracle2hsqldb;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author Moses Hohman
 */
public class HsqldbLibrary {
    private final static TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static Object nvl(Object value, Object defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static String nvl(String value, String defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static Timestamp nvl(Timestamp value, Timestamp defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static Date nvl(Date value, Date defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static BigDecimal nvl(BigDecimal value, BigDecimal defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static Integer nvl(Integer value, Integer defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static Long nvl(Long value, Long defaultValue) {
        return (value==null) ? defaultValue : value;
    }

    public static Date trunc(java.util.Date time) {
        if (time == null) return null;
        
        Calendar param = Calendar.getInstance();
        param.setTime(time);

        Calendar truncated = Calendar.getInstance();
        // note that this isn't quite the same as
        // truncated.setTimeInMillis(0) because
        // this will be in the local TZ
        truncated.set(1970, 1, 1, 0, 0, 0);
        truncated.set(Calendar.YEAR, param.get(Calendar.YEAR));
        truncated.set(Calendar.DAY_OF_YEAR, param.get(Calendar.DAY_OF_YEAR));

        return new Date(truncated.getTimeInMillis());
    }
}
