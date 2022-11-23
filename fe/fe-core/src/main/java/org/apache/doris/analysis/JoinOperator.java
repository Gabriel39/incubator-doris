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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/JoinOperator.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.thrift.TJoinOp;

public class JoinOperator {
    public static JoinOperator INNER_JOIN = new JoinOperator("INNER JOIN", TJoinOp.INNER_JOIN);
    public static JoinOperator LEFT_OUTER_JOIN = new JoinOperator("LEFT OUTER JOIN", TJoinOp.LEFT_OUTER_JOIN);

    public static JoinOperator LEFT_SEMI_JOIN = new JoinOperator("LEFT SEMI JOIN", TJoinOp.LEFT_SEMI_JOIN);
    public static JoinOperator LEFT_ANTI_JOIN = new JoinOperator("LEFT ANTI JOIN", TJoinOp.LEFT_ANTI_JOIN);
    public static JoinOperator RIGHT_SEMI_JOIN = new JoinOperator("RIGHT SEMI JOIN", TJoinOp.RIGHT_SEMI_JOIN);
    public static JoinOperator RIGHT_ANTI_JOIN = new JoinOperator("RIGHT ANTI JOIN", TJoinOp.RIGHT_ANTI_JOIN);
    public static JoinOperator RIGHT_OUTER_JOIN = new JoinOperator("RIGHT OUTER JOIN", TJoinOp.RIGHT_OUTER_JOIN);
    public static JoinOperator FULL_OUTER_JOIN = new JoinOperator("FULL OUTER JOIN", TJoinOp.FULL_OUTER_JOIN);
    public static JoinOperator CROSS_JOIN = new JoinOperator("CROSS JOIN", TJoinOp.CROSS_JOIN);
    // Variant of the LEFT ANTI JOIN that is used for the equal of
    // NOT IN subqueries. It can have a single equality join conjunct
    // that returns TRUE when the rhs is NULL.
    public static JoinOperator NULL_AWARE_LEFT_ANTI_JOIN = new JoinOperator("NULL AWARE LEFT ANTI JOIN",
            TJoinOp.NULL_AWARE_LEFT_ANTI_JOIN);

    private final String  description;
    private final TJoinOp thriftJoinOp;
    private final boolean isMark;
    private final String markTupleName;
    private final String markSlotName;

    private JoinOperator(String description, TJoinOp thriftJoinOp) {
        this(description, thriftJoinOp, null);
    }

    private JoinOperator(String description, TJoinOp thriftJoinOp, TupleDescriptor markTuple) {
        this.description = description;
        this.thriftJoinOp = thriftJoinOp;
        this.isMark = markTuple != null;
        if (isMark) {
            this.markTupleName = markTuple.getAlias();
            this.markSlotName = markTuple.getSlots().get(0).getColumn().getName();
        } else {
            this.markTupleName = null;
            this.markSlotName = null;
        }
    }

    public JoinOperator withMark(TupleDescriptor markTuple) {
        return new JoinOperator(this.description, this.thriftJoinOp, markTuple);
    }

    public boolean isMark() {
        return isMark;
    }

    public String getMarkTupleName() {
        return markTupleName;
    }

    public String getMarkSlotName() {
        return markSlotName;
    }

    @Override
    public String toString() {
        return (isMark ? "MARKED " : "") + description;
    }

    public TJoinOp toThrift() {
        return thriftJoinOp;
    }

    public boolean isOuterJoin() {
        return this == LEFT_OUTER_JOIN || this == RIGHT_OUTER_JOIN || this == FULL_OUTER_JOIN;
    }

    public boolean isSemiAntiJoin() {
        return this == LEFT_SEMI_JOIN || this == RIGHT_SEMI_JOIN || this == LEFT_ANTI_JOIN
                || this == NULL_AWARE_LEFT_ANTI_JOIN || this == RIGHT_ANTI_JOIN;
    }

    public boolean isSemiJoin() {
        return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN
                || this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
                || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isLeftSemiJoin() {
        return this == LEFT_SEMI_JOIN;
    }

    public boolean isInnerJoin() {
        return this == INNER_JOIN;
    }

    public boolean isAntiJoin() {
        return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
                || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isCrossJoin() {
        return this == CROSS_JOIN;
    }

    public boolean isFullOuterJoin() {
        return this == FULL_OUTER_JOIN;
    }

    public boolean isLeftOuterJoin() {
        return this == LEFT_OUTER_JOIN;
    }

    public boolean isRightOuterJoin() {
        return this == RIGHT_OUTER_JOIN;
    }
}
