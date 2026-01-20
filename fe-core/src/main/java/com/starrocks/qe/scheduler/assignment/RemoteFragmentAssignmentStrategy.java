// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler.assignment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.TINY_SCALE_ROWS_LIMIT;

/**
 * The assignment strategy for fragments whose left most node is not a scan node.
 */
public class RemoteFragmentAssignmentStrategy implements FragmentAssignmentStrategy {

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final boolean isGatherOutput;
    private final boolean usePipeline;
    private final boolean enableDopAdaption;
    private final boolean enableSmartDispatch;

    private final Random random;

    public RemoteFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider,
                                            boolean usePipeline, boolean isGatherOutput, Random random) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.usePipeline = usePipeline;
        this.enableDopAdaption = usePipeline && connectContext.getSessionVariable().isEnablePipelineAdaptiveDop();
        this.isGatherOutput = isGatherOutput;
        this.enableSmartDispatch = connectContext.getSessionVariable().isEnableSmartDispatch();
        this.random = random;
    }

    @Override
    public void assignFragmentToWorker(ExecutionFragment execFragment) throws StarRocksException {
        final PlanFragment fragment = execFragment.getPlanFragment();

        // If left child is MultiCastDataFragment(only support left now), will keep same instance with child.
        boolean isCTEConsumerFragment =
                !fragment.getChildren().isEmpty() && fragment.getChild(0) instanceof MultiCastPlanFragment;
        if (isCTEConsumerFragment) {
            assignCTEConsumerFragmentToWorker(execFragment);
            return;
        }

        if (fragment.isGatherFragment()) {
            assignGatherFragmentToWorker(execFragment);
            return;
        }

        assignRemoteFragmentToWorker(execFragment);
    }

    private void assignCTEConsumerFragmentToWorker(ExecutionFragment execFragment) {
        ExecutionFragment childFragment = execFragment.getChild(0);
        for (FragmentInstance childInstance : childFragment.getInstances()) {
            execFragment.addInstance(new FragmentInstance(childInstance.getWorker(), execFragment));
        }
    }

    private void assignGatherFragmentToWorker(ExecutionFragment execFragment) throws StarRocksException {
        Long preferred = getPreferredBackend(execFragment);
        long workerId = preferred != null && workerProvider.getAllAvailableNodes().contains(preferred)
                ? preferred
                : workerProvider.selectNextWorker();
        FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
        execFragment.addInstance(instance);
    }

    private void assignRemoteFragmentToWorker(ExecutionFragment execFragment) {
        final PlanFragment fragment = execFragment.getPlanFragment();

        // hostSet contains target backends to whom fragment instances of the current PlanFragment will be
        // delivered. when pipeline parallelization is adopted, the number of instances should be the size
        // of hostSet, that it to say, each backend has exactly one fragment.
        Set<Long> workerIdSet = Sets.newHashSet();
        int maxParallelism = 0;

        // Smart dispatch: only use recommended backends for SHUFFLE fragments
        // SCAN fragments should use LocalFragmentAssignmentStrategy (handled elsewhere)
        // GATHER fragments should use assignGatherFragmentToWorker (handled above)
        Optional<List<Long>> smartBackends = getSmartDispatchBackends(execFragment);
        boolean useSmartBackends = false;

        if (enableSmartDispatch && smartBackends.isPresent() && !smartBackends.get().isEmpty()) {
            // Check the fragment dispatch category to decide whether to use smart dispatch backends
            // Only SHUFFLE fragments can have their backend selection optimized
            Optional<com.starrocks.qe.scheduler.dispatch.FragmentDispatchContext> dispatchContext =
                    execFragment.getExecutionDAG().getDispatchContext(execFragment.getFragmentId());

            boolean isShuffleFragment = dispatchContext
                    .map(ctx -> ctx.getDispatchCategory() == com.starrocks.qe.scheduler.dispatch.FragmentDispatchContext.DispatchCategory.SHUFFLE)
                    .orElse(false);

            if (isShuffleFragment) {
                List<Long> availableNodes = workerProvider.getAllAvailableNodes();
                List<Long> recommendedBackends = smartBackends.get().stream()
                        .filter(availableNodes::contains)
                        .collect(Collectors.toList());

                if (!recommendedBackends.isEmpty()) {
                    workerIdSet = Sets.newHashSet(recommendedBackends);
                    maxParallelism = workerIdSet.size();
                    useSmartBackends = true;
                }
            }
        }

        // Fall back to original logic if smart dispatch didn't apply
        if (!useSmartBackends) {
            List<Long> selectedComputedNodes = workerProvider.selectAllComputeNodes();
            if (workerProvider.isPreferComputeNode() && !selectedComputedNodes.isEmpty()) {
                workerIdSet = adaptiveChooseNodes(fragment, selectedComputedNodes, Sets.newHashSet(selectedComputedNodes));
                maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
            } else if (fragment.isUnionFragment() && isGatherOutput) {
                for (int i = 0; i < execFragment.childrenSize(); i++) {
                    ExecutionFragment childExecFragment = execFragment.getChild(i);
                    childExecFragment.getInstances().stream()
                            .map(FragmentInstance::getWorkerId)
                            .forEach(workerIdSet::add);
                }
                maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
            } else {
                int maxIndex = 0;
                for (int i = 0; i < execFragment.childrenSize(); i++) {
                    int curInputFragmentParallelism = execFragment.getChild(i).getInstances().size();
                    if (enableDopAdaption) {
                        curInputFragmentParallelism *= fragment.getChild(i).getPipelineDop();
                    }
                    if (curInputFragmentParallelism > maxParallelism) {
                        maxParallelism = curInputFragmentParallelism;
                        maxIndex = i;
                    }
                }

                ExecutionFragment maxFragment = execFragment.getChild(maxIndex);
                Set<Long> childUsedHost = maxFragment.getInstances().stream()
                        .map(FragmentInstance::getWorkerId)
                        .collect(Collectors.toSet());
                workerIdSet = adaptiveChooseNodes(fragment, workerProvider.getAllAvailableNodes(), childUsedHost);

                if (usePipeline) {
                    maxParallelism = workerIdSet.size();
                }
            }
        }

        if (enableDopAdaption) {
            maxParallelism = workerIdSet.size();
        }

        int exchangeInstances = -1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            exchangeInstances = connectContext.getSessionVariable().getExchangeInstanceParallel();
        }
        List<Long> workerIds;
        if (exchangeInstances > 0 && maxParallelism > exchangeInstances) {
            // random select some instance
            // get distinct host, when parallel_fragment_exec_instance_num > 1, single host may execute several instances
            maxParallelism = exchangeInstances;
            workerIds = Lists.newArrayList(workerIdSet);
            Collections.shuffle(workerIds, random);
        } else {
            workerIds = Lists.newArrayList(workerIdSet);
        }

        for (int i = 0; i < maxParallelism; i++) {
            Long workerId = workerIds.get(i % workerIds.size());
            FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
            execFragment.addInstance(instance);
        }

        // When group by cardinality is smaller than number of backend, only some backends always
        // process while other has no data to process.
        // So we shuffle instances to make different backends handle different queries.
        execFragment.shuffleInstances(random);

        // TODO: switch to unpartitioned/coord execution if our input fragment
        // is executed that way (could have been downgraded from distributed)
    }

    private Set<Long> adaptiveChooseNodes(PlanFragment fragment, List<Long> candidates,
                                          Set<Long> childUsedHosts) {
        List<Long> childHosts = Lists.newArrayList(childUsedHosts);

        // sometimes we may reverse the fragment order like SHUFFLE_HASH_BUCKET plan, so we need sort
        // the list to ensure the most left child is at the 0 index position.
        List<PlanFragment> sortedFragments = fragment.getChildren().stream()
                .sorted(Comparator.comparing(e -> e.getPlanRoot().getId().asInt()))
                .collect(Collectors.toList());

        long maxOutputOfRightChild = sortedFragments.stream().skip(1)
                .map(e -> e.getPlanRoot().getCardinality()).reduce(Long::max)
                .orElse(fragment.getChild(0).getPlanRoot().getCardinality());
        long outputOfMostLeftChild = sortedFragments.get(0).getPlanRoot().getCardinality();

        long nodeNums = getOptimalNodeNums(outputOfMostLeftChild, maxOutputOfRightChild, fragment.getPipelineDop(),
                candidates.size());

        SessionVariableConstants.ChooseInstancesMode mode = connectContext.getSessionVariable()
                .getChooseExecuteInstancesMode();
        if (mode.enableIncreaseInstance() && nodeNums > childUsedHosts.size()) {
            for (Long id : candidates) {
                if (!childUsedHosts.contains(id)) {
                    childHosts.add(id);
                    workerProvider.selectWorkerUnchecked(id);
                    if (childHosts.size() == nodeNums) {
                        break;
                    }
                }
            }
            return Sets.newHashSet(childHosts);
        } else if (mode.enableDecreaseInstance() && nodeNums < childUsedHosts.size()
                && candidates.size() >= Config.adaptive_choose_instances_threshold) {
            Collections.shuffle(childHosts, random);
            return childHosts.stream().limit(nodeNums).collect(Collectors.toSet());
        } else {
            return Sets.newHashSet(childHosts);
        }
    }

    private Long getPreferredBackend(ExecutionFragment execFragment) {
        if (!enableSmartDispatch) {
            return null;
        }
        return execFragment.getExecutionDAG().getSmartDispatchBackend(execFragment.getFragmentId()).orElse(null);
    }

    private Optional<List<Long>> getSmartDispatchBackends(ExecutionFragment execFragment) {
        if (!enableSmartDispatch) {
            return Optional.empty();
        }
        return execFragment.getExecutionDAG().getSmartDispatchBackends(execFragment.getFragmentId());
    }

    public static long getOptimalNodeNums(long outputOfMostLeftChild, long maxOutputOfRightChild, int dop, int candidateSize) {
        long baseNodeNums = (long) Math.ceil((double) maxOutputOfRightChild / TINY_SCALE_ROWS_LIMIT / dop);
        double base = Math.max(Math.E, baseNodeNums);

        long amplifyFactor = Math.round(Math.max(1,
                Math.log(outputOfMostLeftChild / TINY_SCALE_ROWS_LIMIT / dop) / Math.log(base)));

        return Math.min(amplifyFactor * baseNodeNums, candidateSize);
    }

}
