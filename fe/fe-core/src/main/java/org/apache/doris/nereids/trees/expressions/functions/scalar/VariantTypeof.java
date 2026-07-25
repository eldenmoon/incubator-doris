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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Return the precise schema of a compute-only Variant V2 value. */
public class VariantTypeof extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(VariantType.INSTANCE)
    );

    public VariantTypeof(Expression variant) {
        super("variant_typeof", variant);
    }

    private VariantTypeof(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public VariantTypeof withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new VariantTypeof(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType inputType = getArgument(0).getDataType();
        if (inputType instanceof NullType) {
            return;
        }
        if (!(inputType instanceof VariantType) || !((VariantType) inputType).isComputeV2()) {
            throw new AnalysisException("variant_typeof requires a Variant V2 input: " + toSql());
        }
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        DataType inputType = getArgument(0).getDataType();
        if (inputType instanceof VariantType && signature.getArgType(0) instanceof VariantType) {
            signature = signature.withArgumentType(0, inputType);
        }
        return super.computeSignature(signature);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
