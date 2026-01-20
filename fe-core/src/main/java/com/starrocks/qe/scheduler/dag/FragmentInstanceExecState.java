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

package com.starrocks.qe.scheduler.dag;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.Counter;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.rpc.AttachmentRequest;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Maintain the single execution of a fragment instance.
 * Executions begin in the state CREATED and transition between states following this diagram:
 *
 * <pre>{@code
 * CREATED ────► DEPLOYING ────► EXECUTING ───► FINISHED
 *                  │                │
 *                  │                │
 *                  │                │
 *                  ├─► CANCELLING ◄─┤
 *                  │       │        │
 *                  │       │        │
 *                  │       ▼        │
 *                  └───► FAILED ◄───┘
 * }
 * </pre>
 * <p>
 * All the methods are thead-safe.
 * The {@link #state} and {@link #profile} are protected by {@code synchronized(this)}.
 */
public class FragmentInstanceExecState {
    private static final Logger LOG = LogManager.getLogger(FragmentInstanceExecState.class);

    private volatile State state = State.CREATED;

    private final JobSpec jobSpec;
    private final PlanFragmentId fragmentId;
    private final TUniqueId instanceId;
    private final int indexInJob;

    /**
     * request and future will be cleaned after deployment completion.
     */
    private TExecPlanFragmentParams requestToDeploy;
    private byte[] serializedRequest;
    private Future<PExecPlanFragmentResult> deployFuture = null;

    private final int fragmentIndex;
    private final RuntimeProfile profile;

    private final ComputeNode worker;
    private final TNetworkAddress address;
    private final long lastMissingHeartbeatTime;

    private FragmentInstance fragmentInstance;
    private boolean trainingRecorded = false;
    private final long createNs = System.nanoTime();
    private long firstReportNs = 0L;
    private long finishReportNs = 0L;
    // Captured memory usage from raw profile before it's modified by QueryRuntimeProfile
    private volatile long capturedMemoryBytes = 0L;

    /**
     * Create a fake backendExecState, only user for stream load profile.
     */
    public static FragmentInstanceExecState createFakeExecution(TUniqueId fragmentInstanceId,
                                                                TNetworkAddress address) {
        String instanceId = DebugUtil.printId(fragmentInstanceId);
        String name = "Instance " + instanceId + " (host=" + address + ")";
        RuntimeProfile profile = new RuntimeProfile(name);
        profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
        profile.addInfoString("InstanceId", instanceId);

        return new FragmentInstanceExecState(null, null, 0, fragmentInstanceId, 0, null, profile, null, null, -1);
    }

    public static FragmentInstanceExecState createExecution(JobSpec jobSpec,
                                                            PlanFragmentId fragmentId,
                                                            int fragmentIndex,
                                                            TExecPlanFragmentParams request,
                                                            ComputeNode worker) {
        TNetworkAddress address = worker.getAddress();
        String instanceId = DebugUtil.printId(request.params.fragment_instance_id);
        String name = "Instance " + instanceId + " (host=" + address + ")";
        RuntimeProfile profile = new RuntimeProfile(name);
        profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
        profile.addInfoString("InstanceId", instanceId);

        return new FragmentInstanceExecState(jobSpec,
                fragmentId, fragmentIndex,
                request.params.getFragment_instance_id(), request.getBackend_num(),
                request,
                profile,
                worker, address, worker.getLastMissingHeartbeatTime());
    }

    private FragmentInstanceExecState(JobSpec jobSpec,
                                      PlanFragmentId fragmentId,
                                      int fragmentIndex,
                                      TUniqueId instanceId,
                                      int indexInJob,
                                      TExecPlanFragmentParams requestToDeploy,
                                      RuntimeProfile profile,
                                      ComputeNode worker,
                                      TNetworkAddress address,
                                      long lastMissingHeartbeatTime) {
        this.jobSpec = jobSpec;
        // fake fragment instance exec state
        if (jobSpec == null) {
            state = State.EXECUTING;
        }
        this.fragmentId = fragmentId;
        this.fragmentIndex = fragmentIndex;
        this.instanceId = instanceId;
        this.indexInJob = indexInJob;

        this.requestToDeploy = requestToDeploy;

        this.profile = profile;

        this.address = address;
        this.worker = worker;
        this.lastMissingHeartbeatTime = lastMissingHeartbeatTime;
    }

    public void serializeRequest() {
        try {
            TSerializer serializer = AttachmentRequest.getSerializer(jobSpec.getPlanProtocol());
            serializedRequest = serializer.serialize(requestToDeploy);
            requestToDeploy = null;
        } catch (TException ignore) {
            // throw exception means serializedRequest will be empty, and then we will treat it as not serialized
        }
    }

    /**
     * Deploy the fragment instance to the worker asynchronously.
     * The state transitions to DEPLOYING.
     */
    public void deployAsync() {
        transitionState(State.CREATED, State.DEPLOYING);

        TNetworkAddress brpcAddress = worker.getBrpcAddress();
        try {
            // when `set enable_plan_serialize_concurrently = false` or encountered exception when serializing.
            if (serializedRequest != null && serializedRequest.length != 0) {
                deployFuture = BackendServiceClient.getInstance().execPlanFragmentAsync(brpcAddress, serializedRequest,
                        jobSpec.getPlanProtocol());
            } else {
                deployFuture = BackendServiceClient.getInstance().execPlanFragmentAsync(brpcAddress, requestToDeploy,
                        jobSpec.getPlanProtocol());
            }
        } catch (RpcException | TException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            deployFuture = new Future<PExecPlanFragmentResult>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public PExecPlanFragmentResult get() {
                    PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                    StatusPB pStatus = new StatusPB();
                    pStatus.errorMsgs = Lists.newArrayList();
                    pStatus.errorMsgs.add(e.getMessage());
                    if (e instanceof RpcException) {
                        // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                        pStatus.statusCode = TStatusCode.THRIFT_RPC_ERROR.getValue();
                    } else {
                        pStatus.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                    }
                    result.status = pStatus;
                    return result;
                }

                @Override
                public PExecPlanFragmentResult get(long timeout, @NotNull TimeUnit unit) {
                    return get();
                }
            };
        } finally {
            serializedRequest = null; // clear serializedRequest after deploy
            requestToDeploy = null; // clear requestToDeploy after deploy
        }
    }

    public static class DeploymentResult {
        private final TStatusCode statusCode;
        private final String errMessage;
        private final Throwable failure;

        public DeploymentResult(TStatusCode statusCode, String errMessage, Throwable failure) {
            this.statusCode = statusCode;
            this.errMessage = errMessage;
            this.failure = failure;
        }

        public Status getStatus() {
            return new Status(statusCode, errMessage);
        }

        public TStatusCode getStatusCode() {
            return statusCode;
        }

        public Throwable getFailure() {
            return failure;
        }
    }

    /**
     * Wait for the response of deployment.
     * The state transitions to EXECUTING or FAILED, if it is at DEPLOYING state.
     *
     * @param deployTimeoutMs The timeout of deployment.
     * @return The deployment result.
     * - With OK status, if the deployment succeeds.
     * - With non-OK status and failure, otherwise.
     */
    public DeploymentResult waitForDeploymentCompletion(long deployTimeoutMs) {
        Preconditions.checkState(State.CREATED != state, "wait for deployment completion before deploying");

        TStatusCode code;
        String errMsg = null;
        Throwable failure = null;
        List<Integer> closedScanNodes = null;
        try {
            PExecPlanFragmentResult result = deployFuture.get(deployTimeoutMs, TimeUnit.MILLISECONDS);
            closedScanNodes = result.getClosedScanNodes();
            code = TStatusCode.findByValue(result.status.statusCode);
            if (!CollectionUtils.isEmpty(result.status.errorMsgs)) {
                errMsg = result.status.errorMsgs.get(0);
            }

        } catch (ExecutionException e) {
            LOG.warn("catch a execute exception", e);
            code = TStatusCode.THRIFT_RPC_ERROR;
            failure = e;
        } catch (InterruptedException e) { // NOSONAR
            LOG.warn("catch a interrupt exception", e);
            code = TStatusCode.INTERNAL_ERROR;
            failure = e;
        } catch (TimeoutException e) {
            LOG.warn("catch a timeout exception", e);
            code = TStatusCode.TIMEOUT;
            errMsg = "deploy query timeout.";
            failure = e;
        }

        MetricRepo.COUNTER_BRPC_EXEC_PLAN_FRAGMENT.increase(1L);
        if (code == TStatusCode.OK) {
            transitionState(State.DEPLOYING, State.EXECUTING);
        } else {
            MetricRepo.COUNTER_BRPC_EXEC_PLAN_FRAGMENT_ERROR.increase(1L);
            transitionState(State.DEPLOYING, State.FAILED);

            if (errMsg == null) {
                errMsg = "exec rpc error.";
            }
            errMsg += " " + String.format("backend [id=%d] [host=%s]", worker.getId(), address.getHostname());

            LOG.warn("exec plan fragment failed, errmsg={}, code={}, fragmentId={}, backend={}:{}",
                    errMsg, code, getFragmentId(), address.hostname, address.port);
        }

        if (closedScanNodes != null) {
            for (int id : closedScanNodes) {
                ScanNode scanNode = fragmentInstance.getExecFragment().getScanNode(new PlanNodeId(id));
                if (scanNode != null) {
                    scanNode.setReachLimit();
                }
            }
        }

        requestToDeploy = null;
        deployFuture = null;
        return new DeploymentResult(code, errMsg, failure);
    }

    /**
     * Update the execution state and profile from the report RPC.
     *
     * @param params The report RPC request.
     * @return true if the state is updated. Otherwise, return false.
     */
    public synchronized boolean updateExecStatus(TReportExecStatusParams params) {
        switch (state) {
            case CREATED:
            case FINISHED: // duplicate packet
            case FAILED:
                return false;
            case DEPLOYING:
            case EXECUTING:
            case CANCELLING:
            default:
                if (params.isDone()) {
                    if (params.getStatus() == null || params.getStatus().getStatus_code() == TStatusCode.OK) {
                        transitionState(State.FINISHED);
                    } else {
                        transitionState(State.FAILED);
                    }
                    finishReportNs = System.nanoTime();
                }
                if (firstReportNs == 0L) {
                    firstReportNs = System.nanoTime();
                }
                return true;
        }
    }

    public synchronized void updateRunningProfile(TReportExecStatusParams execStatusParams) {
        if (execStatusParams.isSetProfile()) {
            // Capture memory from raw thrift profile BEFORE updating
            // This ensures we get the memory value before QueryRuntimeProfile removes it
            if (capturedMemoryBytes == 0L) {
                capturedMemoryBytes = extractMemoryFromThriftProfile(execStatusParams.profile);
            }
            profile.update(execStatusParams.profile);
        }
    }

    /**
     * Extract memory from raw Thrift profile before it's processed.
     * This is called before QueryRuntimeProfile removes certain counters.
     */
    private long extractMemoryFromThriftProfile(TRuntimeProfileTree thriftProfile) {
        if (thriftProfile == null || thriftProfile.nodes == null) {
            return 0L;
        }

        long maxMemory = 0L;
        // Memory counter names to search for in raw thrift profile
        List<String> memCounterNames = Lists.newArrayList(
                "QueryPeakMemoryUsage",
                "QueryAllocatedMemoryUsage",
                "OperatorPeakMemoryUsage",
                "OperatorAllocatedMemoryUsage",
                "PeakMemoryUsage",
                "MemoryUsage"
        );

        for (TRuntimeProfileNode node : thriftProfile.nodes) {
            if (node.counters == null) {
                continue;
            }
            for (TCounter counter : node.counters) {
                if (memCounterNames.contains(counter.name) && counter.value > maxMemory) {
                    maxMemory = counter.value;
                }
            }
        }
        return maxMemory;
    }

    /**
     * Cancel the fragment instance.
     *
     * @param cancelReason The cancel reason.
     * @return true if cancel succeeds. Otherwise, return false.
     */
    public synchronized boolean cancelFragmentInstance(PPlanFragmentCancelReason cancelReason) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "cancelRemoteFragments state={}  backend: {}, fragment instance id={}, reason: {}",
                    state, worker.getId(), DebugUtil.printId(instanceId), cancelReason.name());
        }

        switch (state) {
            case CREATED:
            case CANCELLING:
            case FINISHED:
            case FAILED:
                return false;
            case DEPLOYING: // The cancelling request may arrive earlier than the deployed response.
            case EXECUTING:
            default:
                transitionState(State.CANCELLING);
        }

        TNetworkAddress brpcAddress = worker.getBrpcAddress();
        try {
            BackendServiceClient.getInstance().cancelPlanFragmentAsync(brpcAddress,
                    jobSpec.getQueryId(), instanceId, cancelReason,
                    jobSpec.isEnablePipeline());
        } catch (RpcException e) {
            LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                    brpcAddress.getPort(), e);
            SimpleScheduler.addToBlocklist(worker.getId());
            return false;
        }

        return true;
    }

    public boolean hasBeenDeployed() {
        return state.hasBeenDeployed();
    }

    public boolean isFinished() {
        return state.isTerminal();
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public TUniqueId getInstanceId() {
        return instanceId;
    }

    public Integer getIndexInJob() {
        return indexInJob;
    }

    public ComputeNode getWorker() {
        return worker;
    }

    public void recordTrainingIfNeeded() {
        if (trainingRecorded || fragmentInstance == null || fragmentInstance.getExecFragment() == null) {
            return;
        }
        ExecutionFragment execFragment = fragmentInstance.getExecFragment();
        ExecutionDAG dag = execFragment.getExecutionDAG();
        if (dag == null) {
            return;
        }

        // Get recorder and context
        var recorderOpt = dag.getDispatchTrainingRecorder();
        var contextOpt = dag.getDispatchContext(fragmentId);
        if (recorderOpt.isEmpty() || contextOpt.isEmpty()) {
            return;
        }

        var recorder = recorderOpt.get();
        var context = contextOpt.get();

        // Priority 1: Use actual measured wall clock time from timestamps (most reliable)
        // This works regardless of enable_profile setting
        long wallClockNs = 0L;
        if (finishReportNs > 0 && firstReportNs > 0 && finishReportNs > firstReportNs) {
            wallClockNs = finishReportNs - firstReportNs;
        } else if (finishReportNs > 0 && finishReportNs > createNs) {
            wallClockNs = finishReportNs - createNs;
        } else {
            wallClockNs = Math.max(0, System.nanoTime() - createNs);
        }

        // Priority 2: Try to get CPU time from profile (may be more accurate for CPU-bound queries)
        long profileTimeNs = profile.getCounterTotalTime() != null ? profile.getCounterTotalTime().getValue() : 0L;
        long cpuCostNs = extractCpuNs(profile);

        // Use the best available time estimate:
        // - If profile has meaningful data (> 1ms), use the max of profile time and CPU time
        // - Otherwise, use wall clock time as the baseline
        long bestTimeNs;
        long profileBestNs = Math.max(profileTimeNs, cpuCostNs);
        if (profileBestNs > 1_000_000) { // Profile data seems valid (> 1ms)
            bestTimeNs = profileBestNs;
        } else {
            // Profile data is missing or invalid, use wall clock time
            bestTimeNs = wallClockNs;
        }

        long actualTimeMs = bestTimeNs > 0 ? bestTimeNs / 1_000_000 : 0;

        // For CPU value: prefer actual CPU time from profile, fallback to wall clock time
        long cpuValue = cpuCostNs > 1_000_000 ? cpuCostNs : bestTimeNs;

        // Use captured memory from raw thrift profile (preferred)
        // Fall back to extracting from processed profile if captured value is 0
        long memBytes = capturedMemoryBytes > 0 ? capturedMemoryBytes : extractMemoryBytes(profile);

        // Get expected instance count from the fragment
        int expectedInstanceCount = execFragment.getInstances().size();

        // Add this instance's stats to the aggregator
        var stats = dag.getOrCreateFragmentInstanceStats(fragmentId, expectedInstanceCount);
        boolean allComplete = stats.addInstanceStats(cpuValue, actualTimeMs, memBytes);

        trainingRecorded = true;

        // If all instances have reported, record the aggregated stats
        if (allComplete) {
            recorder.record(context, stats);
        }
    }

    /**
     * Extract memory usage from RuntimeProfile.
     * Looks for peak memory counters used by StarRocks BE.
     *
     * Note: StarRocks BE sends memory counters at different levels:
     * - QueryPeakMemoryUsage: at instance profile level (preferred)
     * - OperatorPeakMemoryUsage: at operator/pipeline level (summed as fallback)
     */
    private long extractMemoryBytes(RuntimeProfile runtimeProfile) {
        if (runtimeProfile == null) {
            return 0L;
        }

        // Priority 1: QueryPeakMemoryUsage is the most accurate counter at instance level
        // This counter is reported by BE and represents memory used by this instance
        Counter queryPeakMem = runtimeProfile.getCounter("QueryPeakMemoryUsage");
        if (queryPeakMem != null && queryPeakMem.getValue() > 0) {
            return queryPeakMem.getValue();
        }

        // Priority 2: Try other instance-level counters
        List<String> instanceCounterNames = Lists.newArrayList(
                "QueryAllocatedMemoryUsage",
                "PeakMemoryUsage",
                "MemoryUsage"
        );
        for (String name : instanceCounterNames) {
            Counter counter = runtimeProfile.getCounter(name);
            if (counter != null && counter.getValue() > 0) {
                return counter.getValue();
            }
        }

        // Priority 3: Sum up OperatorPeakMemoryUsage from all operators in child profiles
        // This is a fallback when instance-level counters are not available
        List<String> operatorCounterNames = Lists.newArrayList(
                "OperatorPeakMemoryUsage",
                "OperatorAllocatedMemoryUsage"
        );
        long operatorMemSum = sumCountersRecursively(runtimeProfile, operatorCounterNames);
        if (operatorMemSum > 0) {
            return operatorMemSum;
        }

        // Priority 4: Find max memory counter from any child profile
        List<String> anyMemCounterNames = Lists.newArrayList(
                "PeakMemoryUsage",
                "AllocatedMemoryUsage",
                "MemoryUsage"
        );
        long maxMem = findMaxCounterRecursively(runtimeProfile, anyMemCounterNames);
        if (maxMem > 0) {
            return maxMem;
        }

        // Log available counters for debugging
        if (LOG.isDebugEnabled()) {
            LOG.debug("No memory counters found in profile for instance {}, available counters at root: {}",
                    DebugUtil.printId(instanceId), runtimeProfile.getCounterMap().keySet());
        }
        return 0L;
    }

    /**
     * Extract CPU time from RuntimeProfile.
     */
    private long extractCpuNs(RuntimeProfile runtimeProfile) {
        if (runtimeProfile == null) {
            return 0L;
        }
        // CPU time counter names used by StarRocks BE
        // Priority: specific CPU counters > operator time > total time
        List<String> cpuCounterNames = Lists.newArrayList(
                "OperatorCpuTime",      // Pipeline operator CPU time
                "ScanCpuTime",          // Scan operator CPU time
                "ExchangeCpuTime",      // Exchange operator CPU time
                "AnalyticCpuTime",      // Analytic operator CPU time
                "SortCpuTime",          // Sort operator CPU time
                "AggregateCpuTime",     // Aggregate operator CPU time
                "JoinCpuTime",          // Join operator CPU time
                "CpuCostNs",            // Generic CPU cost
                "CpuTime",              // Generic CPU time
                "CPUTime",              // Alternative naming
                "cpu_time"              // Snake case naming
        );

        // First, try to sum all CPU time counters across all operators
        long totalCpuNs = sumCountersRecursively(runtimeProfile, cpuCounterNames);
        if (totalCpuNs > 0) {
            return totalCpuNs;
        }

        // Fallback: try to find OperatorTotalTime or TotalTime
        List<String> fallbackNames = Lists.newArrayList(
                "OperatorTotalTime",
                "PipelineTotalTime",
                "TotalTime",
                RuntimeProfile.TOTAL_TIME_COUNTER
        );
        return findMaxCounterRecursively(runtimeProfile, fallbackNames);
    }

    /**
     * Sum all matching counters recursively across the profile tree.
     * This accumulates CPU time from all operators.
     */
    private long sumCountersRecursively(RuntimeProfile profile, List<String> counterNames) {
        if (profile == null) {
            return 0L;
        }
        long sum = 0L;

        // Sum counters at current level
        Map<String, Counter> counterMap = profile.getCounterMap();
        for (String name : counterNames) {
            Counter counter = counterMap.get(name);
            if (counter != null && counter.getValue() > 0) {
                sum += counter.getValue();
            }
        }

        // Recursively sum from children
        for (RuntimeProfile child : profile.getChildMap().values()) {
            sum += sumCountersRecursively(child, counterNames);
        }

        return sum;
    }

    /**
     * Find the maximum value of matching counters recursively.
     * Used as fallback when specific CPU counters are not available.
     */
    private long findMaxCounterRecursively(RuntimeProfile profile, List<String> candidateNames) {
        if (profile == null) {
            return 0L;
        }
        long maxVal = 0L;

        // Check current level
        Map<String, Counter> counterMap = profile.getCounterMap();
        for (String name : candidateNames) {
            Counter counter = counterMap.get(name);
            if (counter != null && counter.getValue() > maxVal) {
                maxVal = counter.getValue();
            }
        }

        // Check children
        for (RuntimeProfile child : profile.getChildMap().values()) {
            long childVal = findMaxCounterRecursively(child, candidateNames);
            if (childVal > maxVal) {
                maxVal = childVal;
            }
        }

        // Final fallback: total time counter
        if (maxVal <= 0) {
            Counter total = profile.getCounterTotalTime();
            if (total != null && total.getValue() > 0) {
                maxVal = total.getValue();
            }
        }

        return maxVal;
    }

    public TNetworkAddress getAddress() {
        return address;
    }

    public int getFragmentIndex() {
        return fragmentIndex;
    }

    public RuntimeProfile getProfile() {
        return profile;
    }

    public synchronized void printProfile(StringBuilder builder) {
        profile.computeTimeInProfile();
        profile.prettyPrint(builder, "");
    }

    public synchronized boolean computeTimeInProfile(int maxFragmentId) {
        if (this.fragmentIndex < 0 || this.fragmentIndex > maxFragmentId) {
            LOG.warn("profileFragmentId {} should be in [0, {})", fragmentIndex, maxFragmentId);
            return false;
        }
        profile.computeTimeInProfile();
        return true;
    }

    public boolean isBackendStateHealthy() {
        if (worker.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime) {
            LOG.warn("backend {} is down while joining the coordinator. job id: {}", worker.getId(),
                    jobSpec.getLoadJobId());
            return false;
        }
        return true;
    }

    public QueryStatisticsItem.FragmentInstanceInfo buildFragmentInstanceInfo() {
        return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                .instanceId(instanceId)
                .fragmentId(String.valueOf(fragmentId))
                .address(address)
                .build();
    }

    public List<TPlanFragmentDestination> getDestinations() {
        if (requestToDeploy == null) {
            return Collections.emptyList();
        }
        if (!requestToDeploy.getParams().getDestinations().isEmpty()) {
            return requestToDeploy.getParams().getDestinations();
        }
        if (requestToDeploy.getFragment().isSetOutput_sink() &&
                requestToDeploy.getFragment().getOutput_sink().isSetMulti_cast_stream_sink()) {
            return requestToDeploy.getFragment().getOutput_sink().getMulti_cast_stream_sink()
                    .getDestinations().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private synchronized void transitionState(State to) {
        state = to;
    }

    private synchronized void transitionState(State from, State to) {
        if (state == from) {
            state = to;
        }
    }

    public State getState() {
        return state;
    }

    public enum State {
        CREATED,
        DEPLOYING,
        EXECUTING,
        CANCELLING,

        FINISHED,
        FAILED;

        public boolean hasBeenDeployed() {
            return this != CREATED;
        }

        public boolean isTerminal() {
            return this == FINISHED || this == FAILED;
        }

        public boolean isFinished() {
            return this == FINISHED;
        }
    }

    public FragmentInstance getFragmentInstance() {
        return fragmentInstance;
    }

    public void setFragmentInstance(FragmentInstance fragmentInstance) {
        this.fragmentInstance = fragmentInstance;
    }

    public TExecPlanFragmentParams getRequestToDeploy() {
        return requestToDeploy;
    }

    public void setRequestToDeploy(TExecPlanFragmentParams requestToDeploy) {
        this.requestToDeploy = requestToDeploy;
    }
}
