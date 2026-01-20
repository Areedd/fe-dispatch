package com.starrocks.qe.scheduler.dispatch;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.planner.PlanFragmentId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GreedyDispatchPolicy implements DispatchPolicy {
    private static final Logger LOG = LogManager.getLogger(GreedyDispatchPolicy.class);

    private static final double MEM_NORMALIZATION_BYTES = 1_000_000_000.0;
    private static final double MEM_LOAD_WEIGHT = 0.3;
    private static final double CPU_LOAD_WEIGHT = 0.7;

    private final CostEstimator costEstimator;
    private final boolean enableShortTaskAcceleration;

    public GreedyDispatchPolicy(CostEstimator costEstimator) {
        this(costEstimator, true);
    }

    public GreedyDispatchPolicy(CostEstimator costEstimator, boolean enableShortTaskAcceleration) {
        this.costEstimator = Objects.requireNonNull(costEstimator, "costEstimator is null");
        this.enableShortTaskAcceleration = enableShortTaskAcceleration;
    }

    @Override
    public DispatchResult dispatch(DispatchRequest request) {
        List<FragmentDispatchContext> contexts = request.getFragmentContexts();
        List<BackendLoadStat> backends = request.getBackendLoadStats();
        Map<PlanFragmentId, List<Long>> fragmentToBackends = new HashMap<>();
        if (contexts.isEmpty() || backends.isEmpty()) {
            return new DispatchResult(fragmentToBackends);
        }

        Map<Long, Double> virtualCpuScore = new HashMap<>();
        Map<Long, Double> virtualMemScore = new HashMap<>();
        Map<Long, Integer> virtualTaskCount = new HashMap<>();

        List<FragmentCandidate> candidates = new ArrayList<>();
        for (FragmentDispatchContext context : contexts) {
            FragmentCost cost = costEstimator.estimate(context);
            candidates.add(new FragmentCandidate(context, cost));
        }

        candidates.sort(Comparator
                .comparing((FragmentCandidate c) -> c.context.getDispatchCategory() == FragmentDispatchContext.DispatchCategory.SHUFFLE ? 0 : 1)
                .thenComparing(Comparator.comparingDouble((FragmentCandidate c) -> c.cost.getPredictedCpuUsagePercent()).reversed()));

        for (FragmentCandidate candidate : candidates) {
            List<Long> selectedBackends = Collections.emptyList();

            switch (candidate.context.getDispatchCategory()) {
                case SCAN:
                    break;

                case GATHER:
                    selectedBackends = handleGatherFragment(candidate, backends, virtualCpuScore, virtualMemScore, virtualTaskCount);
                    break;

                case SHUFFLE:
                default:
                    int targetBackendCount = computeTargetBackendCount(candidate, backends);
                    selectedBackends = selectBackends(
                            candidate, backends, targetBackendCount,
                            virtualCpuScore, virtualMemScore, virtualTaskCount);
                    break;
            }

            fragmentToBackends.put(candidate.context.getFragmentId(), selectedBackends);

            double cpuPerBackend = (candidate.cost.getPredictedCpuUsagePercent() / 100.0d) / selectedBackends.size();
            double memPerBackend = (candidate.cost.getPredictedMemBytes() / MEM_NORMALIZATION_BYTES) / selectedBackends.size();

            for (Long beId : selectedBackends) {
                virtualCpuScore.merge(beId, cpuPerBackend, Double::sum);
                virtualMemScore.merge(beId, memPerBackend, Double::sum);
                virtualTaskCount.merge(beId, 1, Integer::sum);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Fragment {} (category={}) assigned to {} backends, predictedCpu={}%, predictedMem={}MB, predictedTime={}ms, isShort={}",
                        candidate.context.getFragmentId(), candidate.context.getDispatchCategory(),
                        selectedBackends.size(),
                        String.format("%.2f", candidate.cost.getPredictedCpuUsagePercent()),
                        String.format("%.2f", candidate.cost.getPredictedMemBytes() / 1_000_000.0),
                        String.format("%.2f", candidate.cost.getPredictedMaxTimeMs()),
                        candidate.cost.isShortFragment());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("DispatchResult fragmentToBackends={}", fragmentToBackends);
        }
        return new DispatchResult(fragmentToBackends);
    }

    private List<Long> handleGatherFragment(FragmentCandidate candidate, List<BackendLoadStat> backends,
                                            Map<Long, Double> virtualCpuScore,
                                            Map<Long, Double> virtualMemScore,
                                            Map<Long, Integer> virtualTaskCount) {
        List<Long> constrainedWorkers = candidate.context.getConstrainedWorkers();
        if (!constrainedWorkers.isEmpty()) {
            return Collections.singletonList(constrainedWorkers.get(0));
        }

        BackendLoadStat selected = pickLowestCombinedScore(backends, virtualCpuScore, virtualMemScore, new HashSet<>());
        if (selected != null) {
            return Collections.singletonList(selected.getBeId());
        }

        return Collections.singletonList(backends.get(0).getBeId());
    }

    private int computeTargetBackendCount(FragmentCandidate candidate, List<BackendLoadStat> backends) {
        int lowLoadCount = (int) backends.stream()
                .filter(b -> b.getLoadScore() < 0.8)
                .count();

        int targetCount = Math.max(1, lowLoadCount);

        if (enableShortTaskAcceleration && candidate.cost.isShortFragment()) {
            return Math.max(1, targetCount / 2);
        }

        return targetCount;
    }

    private List<Long> selectBackends(FragmentCandidate candidate,
                                       List<BackendLoadStat> backends,
                                       int targetCount,
                                       Map<Long, Double> virtualCpuScore,
                                       Map<Long, Double> virtualMemScore,
                                       Map<Long, Integer> virtualTaskCount) {
        List<Long> selected = new ArrayList<>();
        Set<Long> usedBackends = new HashSet<>();

        List<BackendLoadStat> lowLoadBackends = backends.stream()
                .filter(b -> b.getLoadScore() < 0.8)
                .collect(Collectors.toList());

        List<BackendLoadStat> candidateBackends = lowLoadBackends.isEmpty() ? backends : lowLoadBackends;

        for (int i = 0; i < targetCount && i < candidateBackends.size(); i++) {
            BackendLoadStat backend;
            if (enableShortTaskAcceleration && candidate.cost.isShortFragment()) {
                backend = pickLeastRunning(candidateBackends, virtualTaskCount, usedBackends);
            } else {
                backend = pickLowestCombinedScore(candidateBackends, virtualCpuScore, virtualMemScore, usedBackends);
            }

            if (backend != null) {
                selected.add(backend.getBeId());
                usedBackends.add(backend.getBeId());
            }
        }

        if (selected.isEmpty() && !backends.isEmpty()) {
            selected.add(backends.get(0).getBeId());
        }

        return selected;
    }

    private BackendLoadStat pickLeastRunning(List<BackendLoadStat> backends,
                                              Map<Long, Integer> virtualTaskCount,
                                              Set<Long> excludeBackends) {
        Optional<BackendLoadStat> result = backends.stream()
                .filter(stat -> !excludeBackends.contains(stat.getBeId()))
                .min(Comparator.comparingInt(
                        stat -> stat.getRunningFragmentCount() + virtualTaskCount.getOrDefault(stat.getBeId(), 0)));
        return result.orElse(null);
    }

    private BackendLoadStat pickLowestCombinedScore(List<BackendLoadStat> backends,
                                                     Map<Long, Double> virtualCpuScore,
                                                     Map<Long, Double> virtualMemScore,
                                                     Set<Long> excludeBackends) {
        Optional<BackendLoadStat> result = backends.stream()
                .filter(stat -> !excludeBackends.contains(stat.getBeId()))
                .min(Comparator.comparingDouble(stat -> {
                    long beId = stat.getBeId();
                    double vCpu = virtualCpuScore.getOrDefault(beId, 0.0d);
                    double vMem = virtualMemScore.getOrDefault(beId, 0.0d);
                    double virtualCombined = CPU_LOAD_WEIGHT * vCpu + MEM_LOAD_WEIGHT * vMem;
                    return stat.getLoadScore() + 0.2 * virtualCombined;
                }));
        return result.orElse(null);
    }

    static class FragmentCandidate {
        final FragmentDispatchContext context;
        final FragmentCost cost;

        FragmentCandidate(FragmentDispatchContext context, FragmentCost cost) {
            this.context = context;
            this.cost = cost;
        }
    }
}
