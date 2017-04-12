/*
 * Copyright 2015 Realm Inc.
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

import io.realm.RealmFieldType;
import io.realm.exceptions.RealmMigrationNeededException;


public abstract class ColumnInfo {
    private static final class ColumnDetails {
        public final long columnIndex;
        public final RealmFieldType columnType;
        public final String srcTable;

        public ColumnDetails(long columnIndex, RealmFieldType columnType) {
            this(columnIndex, columnType, null);
        }

        public ColumnDetails(long columnIndex, RealmFieldType columnType, String srcTable) {
            this.columnIndex = columnIndex;
            this.columnType = columnType;
            this.srcTable = srcTable;
        }
    }


    private final Map<String, Long> indicesMap;
    private final boolean mutable;

    protected ColumnInfo(int mapSize) {
        this(new HashMap<String, Long>(mapSize), true);
    }

    protected ColumnInfo(ColumnInfo src, boolean mutable) {
        this((src == null) ? null : new HashMap<>(src.indicesMap), mutable);
    }

    private ColumnInfo(HashMap<String, Long> indicesMap, boolean mutable) {
        this.indicesMap = indicesMap;
        this.mutable = mutable;
    }

    /**
     * Returns a map from column name to column index.
     *
     * @return a map from column name to column index. Do not modify returned map because it may be
     * shared among other {@link ColumnInfo} instances.
     */
    public long getColumnIndex(String columnName) {
        Long index = indicesMap.get(columnName);
        return (index == null) ? -1 : index;
    }

    /**
     * Makes this ColumnInfo an exact copy of {@code src}.
     *
     * @param src The source for the copy.  This instance will be an exact copy of {@code src} after return.
     * {@code src} must not be {@code null}.
     * @throws IllegalArgumentException if {@code other} has different class than this.
     */
    public void copyFrom(ColumnInfo src) {
        if (!mutable) {
            throw new UnsupportedOperationException("Attempt to modify immutable cache");
        }
        if (null == src) {
            throw new NullPointerException("Attempt to copy null ColumnInfo");
        }

        indicesMap.clear();
        indicesMap.putAll(src.indicesMap);
        copy(src, this);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("ColumnInfo[");
        buf.append(mutable).append(",");
        if (indicesMap != null) {
            boolean commaNeeded = false;
            for (Map.Entry<String, Long> entry : indicesMap.entrySet()) {
                if (commaNeeded) { buf.append(","); }
                buf.append(entry.getKey()).append("->").append(entry.getValue());
                commaNeeded = true;
            }
        }
        return buf.append("]").toString();
    }

    /**
     * Return the {@code Class} for the backlink, or null if none exists.
     * Overridden by subclasses that have backlinks.
     *
     * @param name The backlink target field.
     * @return the Class that contains the backlink source field
     */
    public Class<?> getBacklinkSourceClass(String name) {
        return null;
    }

    /**
     * Return the name of the backlink source field, or null if none exists.
     * Overridden by subclasses that have backlinks.
     *
     * @param name The backlink target field.S
     * @return the name of the backlink source field
     */
    public String getBacklinkSourceField(String name) {
        return null;
    }

    /**
     * Create a new object that is an exact copy of {@code src}.
     *
     * @param mutable false to make an immutable copy.
     */
    protected abstract ColumnInfo copy(boolean mutable);

    /**
     * Make {@code dst} into an exact copy of {@code src}.
     * Intended for use only by subclasses.
     *
     * @param src The source for the copy
     * @param dst The destination of the copy.  Will be an exact copy of src after return.
     */
    protected abstract void copy(ColumnInfo src, ColumnInfo dst);

    /**
     * Add a new column to the indexMap.
     * <b>For use only by subclasses!</b>.  Called from proxy constructors.
     *
     * @param table The table to search for the column.
     * @param columnName The name of the column whose index is sought.
     * @param realmPath Realm path, for the error message.
     * @param className Class name, for the error message.
     */
    protected final long addColumnIndex(Table table, String columnName, String realmPath, String className) {
        final long columnIndex = table.getColumnIndex(columnName);
        if (columnIndex < 0) {
            throw new RealmMigrationNeededException(
                    realmPath,
                    "Field '" + columnName + "' not found for type " + className);
        }

        indicesMap.put(columnName, columnIndex);

        return columnIndex;
    }

    /**
     * Returns the {@link Map} that is the implementation for this object.
     * <b>FOR TESTING USE ONLY!</b>
     *
     * @return the corresponding {@link ColumnInfo} object, or {@code null} if not found.
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    //@VisibleForTesting(otherwise = VisibleForTesting.NONE)
    public Map<String, Long> getIndicesMap() {
        return indicesMap;
    }
}
