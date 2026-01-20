package com.starrocks.qe.scheduler.dispatch;

import com.google.common.base.MoreObjects;
import com.starrocks.planner.PlanFragmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FragmentDispatchContext {
    public enum DispatchCategory {
        SCAN,
        GATHER,
        SHUFFLE
    }

    private final PlanFragmentId fragmentId;
    private final String fragmentType;
    private final List<String> operatorTypes;
    private final long estimatedScanRows;
    private final double averageRowSize;
    private final int pipelineDop;
    private final int requiredInstances;
    private final String query;
    private final int joinCount;
    private final int aggKeyCount;
    private final int sortKeyCount;
    private final int predicateCount;
    private final int scanNodeCount;
    private final int planHeight;
    private final long execMemLimitBytes;
    private final int queryTimeoutSeconds;

    private final DispatchCategory dispatchCategory;
    private final List<Long> constrainedWorkers;

    public FragmentDispatchContext(PlanFragmentId fragmentId,
                                   String fragmentType,
                                   List<String> operatorTypes,
                                   long estimatedScanRows,
                                   double averageRowSize,
                                   int pipelineDop,
                                   int requiredInstances,
                                   String query,
                                   int joinCount,
                                   int aggKeyCount,
                                   int sortKeyCount,
                                   int predicateCount,
                                   int scanNodeCount,
                                   int planHeight,
                                   long execMemLimitBytes,
                                   int queryTimeoutSeconds) {
        this(fragmentId, fragmentType, operatorTypes, estimatedScanRows, averageRowSize, pipelineDop,
                requiredInstances, query, joinCount, aggKeyCount, sortKeyCount, predicateCount,
                scanNodeCount, planHeight, execMemLimitBytes, queryTimeoutSeconds,
                DispatchCategory.SHUFFLE, Collections.emptyList());
    }

    public FragmentDispatchContext(PlanFragmentId fragmentId,
                                   String fragmentType,
                                   List<String> operatorTypes,
                                   long estimatedScanRows,
                                   double averageRowSize,
                                   int pipelineDop,
                                   int requiredInstances,
                                   String query,
                                   int joinCount,
                                   int aggKeyCount,
                                   int sortKeyCount,
                                   int predicateCount,
                                   int scanNodeCount,
                                   int planHeight,
                                   long execMemLimitBytes,
                                   int queryTimeoutSeconds,
                                   DispatchCategory dispatchCategory,
                                   List<Long> constrainedWorkers) {
        this.fragmentId = Objects.requireNonNull(fragmentId, "fragmentId is null");
        this.fragmentType = Objects.requireNonNull(fragmentType, "fragmentType is null");
        List<String> operatorTypeCopy = operatorTypes == null ? Collections.emptyList() : new ArrayList<>(operatorTypes);
        this.operatorTypes = Collections.unmodifiableList(operatorTypeCopy);
        this.estimatedScanRows = estimatedScanRows;
        this.averageRowSize = averageRowSize;
        this.pipelineDop = pipelineDop;
        this.requiredInstances = requiredInstances;
        this.query = query == null ? "" : query;
        this.joinCount = joinCount;
        this.aggKeyCount = aggKeyCount;
        this.sortKeyCount = sortKeyCount;
        this.predicateCount = predicateCount;
        this.scanNodeCount = scanNodeCount;
        this.planHeight = planHeight;
        this.execMemLimitBytes = execMemLimitBytes;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.dispatchCategory = dispatchCategory;
        List<Long> workersCopy = constrainedWorkers == null ? Collections.emptyList() : new ArrayList<>(constrainedWorkers);
        this.constrainedWorkers = Collections.unmodifiableList(workersCopy);
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public String getFragmentType() {
        return fragmentType;
    }

    public List<String> getOperatorTypes() {
        return operatorTypes;
    }

    public long getEstimatedScanRows() {
        return estimatedScanRows;
    }

    public double getAverageRowSize() {
        return averageRowSize;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public int getRequiredInstances() {
        return requiredInstances;
    }

    public String getQuery() {
        return query;
    }

    public int getJoinCount() {
        return joinCount;
    }

    public int getAggKeyCount() {
        return aggKeyCount;
    }

    public int getSortKeyCount() {
        return sortKeyCount;
    }

    public int getPredicateCount() {
        return predicateCount;
    }

    public int getScanNodeCount() {
        return scanNodeCount;
    }

    public int getPlanHeight() {
        return planHeight;
    }

    public long getExecMemLimitBytes() {
        return execMemLimitBytes;
    }

    public int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public DispatchCategory getDispatchCategory() {
        return dispatchCategory;
    }

    public List<Long> getConstrainedWorkers() {
        return constrainedWorkers;
    }

    public boolean hasWorkerConstraints() {
        return !constrainedWorkers.isEmpty();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("fragmentId", fragmentId)
                .add("fragmentType", fragmentType)
                .add("operatorTypes", operatorTypes)
                .add("estimatedScanRows", estimatedScanRows)
                .add("averageRowSize", averageRowSize)
                .add("pipelineDop", pipelineDop)
                .add("requiredInstances", requiredInstances)
                .add("query", query)
                .add("joinCount", joinCount)
                .add("aggKeyCount", aggKeyCount)
                .add("sortKeyCount", sortKeyCount)
                .add("predicateCount", predicateCount)
                .add("scanNodeCount", scanNodeCount)
                .add("planHeight", planHeight)
                .add("execMemLimitBytes", execMemLimitBytes)
                .add("queryTimeoutSeconds", queryTimeoutSeconds)
                .add("dispatchCategory", dispatchCategory)
                .add("constrainedWorkers", constrainedWorkers)
                .toString();
    }
}
