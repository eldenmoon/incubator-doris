// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.ComplexDataType;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 *
 * ComplexVariant type in Nereids which could predefine some fields of nested columns.
 * Example: VARIANT <`a.b`:INT, a.c:DATETIMEV2>
 *
 */
@Developing
public class ComplexVariantType extends DataType implements ComplexDataType {
    // for test compatibility

    public static final List<StructField> TEST_PREDEFINE_LIST = new ArrayList<StructField>() {
        {
            add(new StructField("PREDEFINE_COL1", StringType.INSTANCE, true, "", true));
            add(new StructField("PREDEFINE_COL2", IntegerType.INSTANCE, true, "", true));
            add(new StructField("PREDEFINE_COL3", DecimalV3Type.SYSTEM_DEFAULT, true, "", true));
            add(new StructField("PREDEFINE_COL4", DateTimeV2Type.SYSTEM_DEFAULT, true, "", true));
        }
    };
    public static final ComplexVariantType TEST_INSTANCE = new ComplexVariantType(TEST_PREDEFINE_LIST);
    // predefined fields
    private final List<StructField> predefinedFields;
    private final Supplier<Map<String, StructField>> pathToFields;

    // No predefined fields
    public ComplexVariantType() {
        predefinedFields = Lists.newArrayList();
        pathToFields = Suppliers.memoize(Maps::newTreeMap);
    }

    /**
     *   Contains predefined fields like struct
     */
    public ComplexVariantType(List<StructField> fields) {
        this.predefinedFields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.pathToFields = Suppliers.memoize(() -> this.predefinedFields.stream().collect(ImmutableMap.toImmutableMap(
                StructField::getName, f -> f, (f1, f2) -> {
                    throw new AnalysisException("The name of the struct field cannot be repeated."
                            + " same name fields are " + f1 + " and " + f2);
                })));
    }

    public Map<String, StructField> getPathToFields() {
        return pathToFields.get();
    }

    public List<StructField> getPredefinedFields() {
        return predefinedFields;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof VariantType || other instanceof ComplexVariantType;
    }

    @Override
    public Type toCatalogDataType() {
        return predefinedFields.isEmpty() ? Type.VARIANT
                : new org.apache.doris.catalog.ComplexVariantType(predefinedFields.stream()
                        .map(StructField::toCatalogDataType)
                        .collect(Collectors.toCollection(ArrayList::new)));
    }

    @Override
    public String toSql() {
        if (predefinedFields.isEmpty()) {
            return "VARIANT";
        }
        return "VARIANT<" + predefinedFields.stream().map(StructField::toSql).collect(Collectors.joining(",")) + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public int width() {
        return 24;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
