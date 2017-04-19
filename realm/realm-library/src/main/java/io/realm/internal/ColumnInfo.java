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
        public final long srcColumn;

        public ColumnDetails(long columnIndex, RealmFieldType columnType, String srcTable, long srcColumn) {
            this.columnIndex = columnIndex;
            this.columnType = columnType;
            this.srcTable = srcTable;
            this.srcColumn = srcColumn;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder("ColumnDetails[");
            buf.append(columnIndex);
            buf.append(", ").append(columnType);
            if (srcTable != null) {
                buf.append(", ").append(srcTable);
                buf.append(", ").append(srcColumn);
            }
            return buf.append("]").toString();
        }
    }


    private final Map<String, ColumnDetails> indicesMap;
    private final boolean mutable;

    protected ColumnInfo(int mapSize) {
        this(new HashMap<String, ColumnDetails>(mapSize), true);
    }

    protected ColumnInfo(ColumnInfo src, boolean mutable) {
        this((src == null) ? null : new HashMap<>(src.indicesMap), mutable);
    }

    private ColumnInfo(HashMap<String, ColumnDetails> indicesMap, boolean mutable) {
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
        ColumnDetails details = indicesMap.get(columnName);
        return (details == null) ? -1 : details.columnIndex;
    }

    public RealmFieldType getColumnType(String columnName) {
        ColumnDetails details = indicesMap.get(columnName);
        return (details == null) ? RealmFieldType.UNSUPPORTED_TABLE : details.columnType;
    }

    public String getSourceTable(String columnName) {
        ColumnDetails details = indicesMap.get(columnName);
        return (details == null) ? null : details.srcTable;
    }

    public long getSourceColumnIndex(String columnName) {
        ColumnDetails details = indicesMap.get(columnName);
        return (details == null) ? -1 : details.srcColumn;
    }

    /**
     * Makes this ColumnInfo an exact copy of {@code src}.
     *
     * @param src The source for the copy.  This instance will be an exact copy of {@code src} after return.
     * {@code src} must not be {@code null}.
     * @throws IllegalArgumentException if {@code other} has different class than this.
     */
    public final void copyFrom(ColumnInfo src) {
        if (!mutable) {
            throw new UnsupportedOperationException("Attempt to modify immutable cache");
        }
        if (null == src) {
            throw new NullPointerException("Attempt to copy null ColumnInfo");
        }

        indicesMap.clear();
        indicesMap.putAll(src.indicesMap);  // ColumnDetails are immutable: no need to copy
        copy(src, this);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("ColumnInfo[");
        buf.append(mutable).append(",");
        if (indicesMap != null) {
            boolean commaNeeded = false;
            for (Map.Entry<String, ColumnDetails> entry : indicesMap.entrySet()) {
                if (commaNeeded) { buf.append(","); }
                buf.append(entry.getKey()).append("->").append(entry.getValue());
                commaNeeded = true;
            }
        }
        return buf.append("]").toString();
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
     * <b>For use only by subclasses!</b>.
     * Must be called from within constructor, to keep effectively-final contract.
     *
     * @param realmPath Realm path, for the error message.
     * @param table The table to search for the column.
     * @param columnName The name of the column whose index is sought.
     * @param columnType Type for the column.
     */
    @SuppressWarnings("unused")
    protected final long addColumnDetails(String realmPath, Table table, String columnName, RealmFieldType columnType) {
        return addColumnDetails(realmPath, table, columnName, columnType, null, null);
    }

    /**
     * Add a new backlinked column to the indexMap.
     * <b>For use only by subclasses!</b>
     * Must be called from within constructor, to keep effectively-final contract.
     *
     * @param realmPath Realm path, for the error message.
     * @param table The table to search for the column.
     * @param columnName The name of the column whose index is sought.
     * @param columnType Type for the column.
     * @param srcTableName The name of the backlink source table.
     * @param srcColumnName Name of the backlink source column.
     */
    @SuppressWarnings("unused")
    protected final long addColumnDetails(String realmPath, Table table, String columnName, RealmFieldType columnType, String srcTableName, String srcColumnName) {
        final long columnIndex = table.getColumnIndex(columnName);
        if (columnIndex < 0) {
            throw new RealmMigrationNeededException(
                    realmPath,
                    "Field '" + columnName + "' not found for type " + Table.getClassNameForTable(table.getName()));
        }
        RealmFieldType actualColumnType = table.getColumnType(columnIndex);
        if (actualColumnType != columnType) {
            throw new RealmMigrationNeededException(
                    realmPath,
                    "Field '" + columnName + "': expected type " + columnType + "but found " + actualColumnType);
        }

        long srcColumnIndex = 0L;

        indicesMap.put(columnName, new ColumnDetails(columnIndex, columnType, srcTableName, srcColumnIndex));

        return columnIndex;
    }

    /**
     * Returns the {@link Map} that is the implementation for this object.
     * <b>FOR TESTING USE ONLY!</b>
     *
     * @return the corresponding {@link ColumnInfo} object, or {@code null} if not found.
     */
    @SuppressWarnings({"ReturnOfCollectionOrArrayField", "unused"})
    //@VisibleForTesting(otherwise = VisibleForTesting.NONE)
    public Map<String, ColumnDetails> getIndicesMap() {
        return indicesMap;
    }
}
