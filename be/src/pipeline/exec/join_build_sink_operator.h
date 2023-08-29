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

#pragma once

#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "vec/exec/join/vjoin_node_base.h"

namespace doris {

namespace pipeline {

class JoinBuildSinkOperatorX;

template <typename DependencyType>
class JoinBuildSinkLocalState : public PipelineXSinkLocalState<DependencyType> {
    ENABLE_FACTORY_CREATOR(JoinBuildSinkLocalState);

public:
    JoinBuildSinkLocalState(DataSinkOperatorX* parent, RuntimeState* state)
            : PipelineXSinkLocalState<DependencyType>(parent, state) {}

    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXSinkLocalState<DependencyType>::init(state, info));
        auto& p = PipelineXSinkLocalState<DependencyType>::_parent
                          ->template cast<JoinBuildSinkOperatorX>();

        PipelineXSinkLocalState<DependencyType>::profile()->add_info_string("JoinType",
                                                                            to_string(p._join_op));
        _build_phase_profile = PipelineXSinkLocalState<DependencyType>::profile()->create_child(
                "BuildPhase", true, true);
        _build_get_next_timer = ADD_TIMER(_build_phase_profile, "BuildGetNextTime");
        _build_timer = ADD_TIMER(_build_phase_profile, "BuildTime");
        _build_rows_counter = ADD_COUNTER(_build_phase_profile, "BuildRows", TUnit::UNIT);

        _push_down_timer = ADD_TIMER(PipelineXSinkLocalState<DependencyType>::profile(),
                                     "PublishRuntimeFilterTime");
        _push_compute_timer = ADD_TIMER(PipelineXSinkLocalState<DependencyType>::profile(),
                                        "PushDownComputeTime");

        return Status::OK();
    }

protected:
    friend class JoinBuildSinkOperatorX;

    bool _short_circuit_for_null_in_probe_side = false;

    RuntimeProfile* _build_phase_profile;
    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_get_next_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
};

class JoinBuildSinkOperatorX : public DataSinkOperatorX {
public:
    JoinBuildSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~JoinBuildSinkOperatorX() = default;

protected:
    void _init_join_op();
    template <typename DependencyType>
    friend class JoinBuildSinkLocalState;

    TJoinOp::type _join_op;
    vectorized::JoinOpVariants _join_op_variants;

    const bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    bool _build_unique;          // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_left_semi_anti;
    const bool _is_outer_join;
    const bool _is_mark_join;

    // For null aware left anti join, we apply a short circuit strategy.
    // 1. Set _short_circuit_for_null_in_build_side to true if join operator is null aware left anti join.
    // 2. In build phase, we stop materialize build side when we meet the first null value and set _short_circuit_for_null_in_probe_side to true.
    // 3. In probe phase, if _short_circuit_for_null_in_probe_side is true, join node returns empty block directly. Otherwise, probing will continue as the same as generic left anti join.
    const bool _short_circuit_for_null_in_build_side;
};

} // namespace pipeline
} // namespace doris
