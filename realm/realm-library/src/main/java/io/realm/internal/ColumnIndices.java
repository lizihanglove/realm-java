/*
 * Copyright 2014 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.realm.internal;

import java.util.HashMap;
import java.util.Map;

import io.realm.RealmModel;


/**
 * Utility class used to cache the mapping between object field names and their column indices.
 */
public final class ColumnIndices {
    private final Map<Class<? extends RealmModel>, ColumnInfo> classes;
    private final boolean mutable;
    private long schemaVersion;

    public ColumnIndices(long schemaVersion, Map<Class<? extends RealmModel>, ColumnInfo> classes) {
        this(schemaVersion, new HashMap<>(classes), true);
    }

    public ColumnIndices(ColumnIndices other, boolean mutable) {
        this(other.schemaVersion, new HashMap<Class<? extends RealmModel>, ColumnInfo>(other.classes.size()), mutable);
        for (Map.Entry<Class<? extends RealmModel>, ColumnInfo> entry : other.classes.entrySet()) {
            this.classes.put(entry.getKey(), entry.getValue().copy(mutable));
        }
    }

    private ColumnIndices(long schemaVersion, Map<Class<? extends RealmModel>, ColumnInfo> classes, boolean mutable) {
        this.schemaVersion = schemaVersion;
        this.classes = classes;
        this.mutable = mutable;
    }

    /**
     * Get the schema version.
     *
     * @return the schema version.
     */
    public long getSchemaVersion() {
        return schemaVersion;
    }

    /**
     * Returns the {@link ColumnInfo} for the passed class ({@code null} if there is no such class).
     *
     * @param clazz the class for which to get the ColumnInfo.
     * @return the corresponding {@link ColumnInfo} object, or {@code null} if not found.
     */
    public ColumnInfo getColumnInfo(Class<? extends RealmModel> clazz) {
        return classes.get(clazz);
    }

    /**
     * Convenience method to return the column index for a given field on a clazz
     * or {@code -1} if no such field exists.
     *
     * @param clazz the class to search.
     * @param fieldName the name of the field whose index is needed.
     * @return the index in clazz of the field fieldName.
     */
    public long getColumnIndex(Class<? extends RealmModel> clazz, String fieldName) {
        final ColumnInfo columnInfo = getColumnInfo(clazz);
        if (columnInfo == null) {
            return -1;
        }
        return columnInfo.getColumnIndex(fieldName);
    }

    public void copyFrom(ColumnIndices other, RealmProxyMediator mediator) {
        if (!mutable) {
            throw new UnsupportedOperationException("Attempt to modify immutable cache");
        }
        Map<Class<? extends RealmModel>, ColumnInfo> otherClasses = other.classes;
        for (Map.Entry<Class<? extends RealmModel>, ColumnInfo> entry : classes.entrySet()) {
            final ColumnInfo otherColumnInfo = otherClasses.get(entry.getKey());
            if (otherColumnInfo == null) {
                throw new IllegalStateException("Failed to copy ColumnIndices cache: "
                        + Table.tableNameToClassName(mediator.getTableName(entry.getKey())));
            }
            entry.getValue().copyFrom(otherColumnInfo);
        }
        this.schemaVersion = other.schemaVersion;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("ColumnIndices[");
        buf.append(schemaVersion).append(",");
        buf.append(mutable).append(",");
        if (classes != null) {
            boolean commaNeeded = false;
            for (Map.Entry<Class<? extends RealmModel>, ColumnInfo> entry : classes.entrySet()) {
                if (commaNeeded) { buf.append(","); }
                buf.append(entry.getKey().getSimpleName()).append("->").append(entry.getValue());
                commaNeeded = true;
            }
        }
        return buf.append("]").toString();
    }
}
