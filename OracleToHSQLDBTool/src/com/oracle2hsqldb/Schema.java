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

import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Moses Hohman
 */
public class Schema {
    private String name;
    private List tables = new LinkedList();
    private List sequences = new LinkedList();

    public Schema(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public List tables() {
        return Collections.unmodifiableList(tables);
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public Table findTable(String name) {
        Iterator tabs = tables.iterator();
        while (tabs.hasNext()) {
            Table tab = (Table) tabs.next();
            if (tab.name().equals(name)) {
                return tab;
            }
        }
        return null;
    }

    public List sequences() {
        return Collections.unmodifiableList(sequences);
    }

    public void addSequence(Sequence seq) {
        sequences.add(seq);
    }
}
