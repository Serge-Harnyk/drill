/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import java.util.List;
import java.util.logging.Logger;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.exec.planner.logical.DrillUnionRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class UnionAllPrule extends Prule {
  public static final RelOptRule INSTANCE = new UnionAllPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private UnionAllPrule() {
    super(
        RelOptHelper.any(DrillUnionRel.class), "Prel.UnionAllPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DrillUnionRel union = (DrillUnionRel) call.rel(0);
    return (! union.isDistinct());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillUnionRel union = (DrillUnionRel) call.rel(0);
    final List<RelNode> inputs = union.getInputs();
    List<RelNode> convertedInputList = Lists.newArrayList();
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

    try {
      for (int i = 0; i < inputs.size(); i++) {
        RelNode convertedInput = convert(inputs.get(i), PrelUtil.fixTraits(call, traits));
        convertedInputList.add(convertedInput);
      }

      // output distribution trait is set to ANY since union-all inputs may be distributed in different ways
      // and unlike a join there are no join keys that allow determining how the output would be distributed.
      // Note that a downstream operator may impose a required distribution which would be satisfied by
      // inserting an Exchange after the Union-All.
      traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.ANY);

      Preconditions.checkArgument(convertedInputList.size() >= 2, "Union list must be at least two items.");
      RelNode left = convertedInputList.get(0);
      for (int i = 1; i < convertedInputList.size(); i++) {
        left = new UnionAllPrel(union.getCluster(), traits, ImmutableList.of(left, convertedInputList.get(i)),
            false /* compatibility already checked during logical phase */);

      }
      call.transformTo(left);

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

}
