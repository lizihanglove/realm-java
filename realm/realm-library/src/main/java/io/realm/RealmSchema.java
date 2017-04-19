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

package io.realm;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.realm.internal.ColumnIndices;
import io.realm.internal.ColumnInfo;
import io.realm.internal.RealmProxyMediator;
import io.realm.internal.Table;


/**
 * Class for interacting with the Realm schema using a dynamic API. This makes it possible
 * to add, delete and change the classes in the Realm.
 * <p>
 * All changes must happen inside a write transaction for the particular Realm.
 *
 * @see RealmMigration
 */
public abstract class RealmSchema {
    protected ColumnIndices columnIndices; // Cached field look up

    /**
     * Release the schema and any of native resources it might hold.
     */
    public abstract void close();

    /**
     * Returns the Realm schema for a given class.
     *
     * @param className name of the class
     * @return schema object for that class or {@code null} if the class doesn't exists.
     */
    public abstract RealmObjectSchema get(String className);

    /**
     * Returns the {@link RealmObjectSchema}s for all RealmObject classes that can be saved in this Realm.
     *
     * @return the set of all classes in this Realm or no RealmObject classes can be saved in the Realm.
     */
    public abstract Set<RealmObjectSchema> getAll();

    /**
     * Adds a new class to the Realm.
     *
     * @param className name of the class.
     * @return a Realm schema object for that class.
     */
    public abstract RealmObjectSchema create(String className);

    /**
     * Removes a class from the Realm. All data will be removed. Removing a class while other classes point
     * to it will throw an {@link IllegalStateException}. Removes those classes or fields first.
     *
     * @param className name of the class to remove.
     */
    public abstract void remove(String className);

    /**
     * Renames a class already in the Realm.
     *
     * @param oldClassName old class name.
     * @param newClassName new class name.
     * @return a schema object for renamed class.
     */
    public abstract RealmObjectSchema rename(String oldClassName, String newClassName);

    /**
     * Checks if a given class already exists in the schema.
     *
     * @param className class name to check.
     * @return {@code true} if the class already exists. {@code false} otherwise.
     */
    public abstract boolean contains(String className);

    /**
     * Parses the passed filed description (@see parseFieldDescription(String) and returns the information
     * necessary for RealmQuery predicates to select the specified records.
     * Because the values returned by this method will, immediately, be handed to native code, they are
     * in coordinated arrays, not a List&lt;ColInfo&gt;
     * There are two kinds of records.  If return[1][i] is NativeObject.NULLPTR, return[0][i] contains
     * the column index for the i-th element in the dotted field description path.
     * If return[1][i] is *not* NativeObject.NULLPTR, it is the pointer to the source table for a backlink
     * and return[0][i] is the column index of the source column in that table.
     *
     * @param table the starting Table: where(Table.class)
     * @param fieldDescription fieldName or link path to a field name.
     * @param validColumnTypes valid field type for the last field in a linked field
     * @return a pair of arrays:  [0] is column indices, [1] is either NativeObject.NULLPTR or a native table pointer.
     */
    abstract long[][] getColumnIndices(Table table, String fieldDescription, RealmFieldType[] validColumnTypes);

    abstract Table getTable(Class<? extends RealmModel> clazz);

    abstract Table getTable(String className);

    abstract RealmObjectSchema getSchemaForClass(Class<? extends RealmModel> clazz);

    abstract RealmObjectSchema getSchemaForClass(String className);

    /**
     * Parse the passed field description into its components.
     * This must be standard across impleentations and is, therefore, implemented in the base class.
     *
     * @param fieldDescription a field description.
     * @return the parse tree: a list of column names
     */
    protected final List<String> parseFieldDescription(String fieldDescription) {
        if (fieldDescription == null || fieldDescription.equals("")) {
            throw new IllegalArgumentException("Invalid query: field name is empty");
        }
        if (fieldDescription.endsWith(".")) {
            throw new IllegalArgumentException("Invalid query: field name must not end with a period ('.')");
        }
        return Arrays.asList(fieldDescription.split("\\."));
    }

    /**
     * Set the column index cache for this schema.
     *
     * @param columnIndices the column index cache
     */
    final void setInitialColumnIndices(ColumnIndices columnIndices) {
        if (this.columnIndices != null) {
            throw new IllegalStateException("An instance of ColumnIndices is already set.");
        }
        this.columnIndices = new ColumnIndices(columnIndices, true);
    }


    /**
     * Set the column index cache for this schema.
     *
     * @param version the schema version
     * @param columnInfoMap the column info map
     */
    final void setInitialColumnIndices(long version, Map<Class<? extends RealmModel>, ColumnInfo> columnInfoMap) {
        if (this.columnIndices != null) {
            throw new IllegalStateException("An instance of ColumnIndices is already set.");
        }
        columnIndices = new ColumnIndices(version, columnInfoMap);
    }

    /**
     * Updates all {@link ColumnInfo} elements in {@code columnIndices}.
     * <p>
     * <p>
     * The ColumnInfo elements are shared between all {@link RealmObject}s created by the Realm instance
     * which owns this RealmSchema. Updating them also means updating indices information in those {@link RealmObject}s.
     *
     * @param schemaVersion new schema version.
     * @param mediator mediator for the Realm.
     */
    void updateColumnIndices(ColumnIndices schemaVersion, RealmProxyMediator mediator) {
        columnIndices.copyFrom(schemaVersion, mediator);
    }

    /**
     * Sometimes you need ColumnIndicies that can be passed between threads.
     * Setting the mutable flag false creates an instance that is effectively final.
     *
     * @return a new, thread-safe copy of this Schema's ColumnIndices.
     */
    final ColumnIndices getImmutableColumnIndicies() {
        checkIndices();
        return new ColumnIndices(columnIndices, false);
    }

    final ColumnInfo getColumnInfo(Class<? extends RealmModel> clazz) {
        checkIndices();
        return columnIndices.getColumnInfo(clazz);
    }

    final long getSchemaVersion() {
        checkIndices();
        return this.columnIndices.getSchemaVersion();
    }

    final boolean isProxyClass(Class<? extends RealmModel> modelClass, Class<? extends RealmModel> testee) {
        return modelClass.equals(testee);
    }

    private void checkIndices() {
        if (this.columnIndices == null) {
            throw new IllegalStateException("Attempt to use column index before set.");
        }
    }
}
