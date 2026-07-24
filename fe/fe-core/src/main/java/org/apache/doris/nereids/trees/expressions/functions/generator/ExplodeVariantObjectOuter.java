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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Outer form of explode_variant_object. */
public class ExplodeVariantObjectOuter extends TableGeneratingFunction
        implements UnaryExpression, CustomSignature, AlwaysNullable {

    public ExplodeVariantObjectOuter(Expression arg) {
        super("explode_variant_object_outer", arg);
    }

    private ExplodeVariantObjectOuter(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public ExplodeVariantObjectOuter withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ExplodeVariantObjectOuter(getFunctionParams(children));
    }

    @Override
    public FunctionSignature customSignature() {
        DataType inputType = getArgument(0).getDataType();
        if (!(inputType instanceof VariantType)) {
            SearchSignature.throwCanNotFoundFunctionException(getName(), getArguments());
        }
        return FunctionSignature.ret(StructLiteral.constructStructType(
                ImmutableList.of(StringType.INSTANCE, JsonType.INSTANCE))).args(inputType);
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeVariantObjectOuter(this, context);
    }
}
