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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SortNode;
import com.starrocks.planner.TableFunctionTableSink;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.LazyWorkerProvider;
import com.starrocks.qe.scheduler.TFragmentInstanceFactory;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategyFactory;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.dispatch.BackendLoadStat;
import com.starrocks.qe.scheduler.dispatch.CostEstimator;
import com.starrocks.qe.scheduler.dispatch.DispatchRequest;
import com.starrocks.qe.scheduler.dispatch.DispatchResult;
import com.starrocks.qe.scheduler.dispatch.FragmentDispatchContext;
import com.starrocks.qe.scheduler.dispatch.FragmentDispatcher;
import com.starrocks.qe.scheduler.dispatch.GreedyDispatchPolicy;
import com.starrocks.qe.scheduler.dispatch.PythonModelPredictor;
import com.starrocks.qe.scheduler.dispatch.DispatchTrainingRecorder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.nio.file.Path;

public class CoordinatorPreprocessor {
    private static final Logger LOG = LogManager.getLogger(CoordinatorPreprocessor.class);
    private static final String LOCAL_IP = FrontendOptions.getLocalHostAddress();
    public static final int BUCKET_ABSENT = 2147483647;

    static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final TNetworkAddress coordAddress;
    private final ConnectContext connectContext;

    private final JobSpec jobSpec;
    private final boolean enablePhasedSchedule;
    private final ExecutionDAG executionDAG;

    private final WorkerProvider.Factory workerProviderFactory;
    private LazyWorkerProvider lazyWorkerProvider;

    private final FragmentAssignmentStrategyFactory fragmentAssignmentStrategyFactory;

    // Cached data for recording after assignment completes
    private List<BackendLoadStat> lastBackendLoadStats;
    private DispatchResult lastDispatchResult;

    public CoordinatorPreprocessor(ConnectContext context, JobSpec jobSpec, boolean enablePhasedSchedule) {
        workerProviderFactory = newWorkerProviderFactory();
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = Preconditions.checkNotNull(context);
        this.jobSpec = jobSpec;
        this.enablePhasedSchedule = enablePhasedSchedule;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.lazyWorkerProvider = LazyWorkerProvider.of(() -> workerProviderFactory.captureAvailableWorkers(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes(),
                sessionVariable.getComputationFragmentSchedulingPolicy(), jobSpec.getComputeResource()));

        this.fragmentAssignmentStrategyFactory = new FragmentAssignmentStrategyFactory(connectContext, jobSpec, executionDAG);

    }

    @VisibleForTesting
    CoordinatorPreprocessor(List<PlanFragment> fragments, List<ScanNode> scanNodes, ConnectContext context) {
        workerProviderFactory = newWorkerProviderFactory();
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = context;
        this.jobSpec = JobSpec.Factory.mockJobSpec(connectContext, fragments, scanNodes);
        this.enablePhasedSchedule = false;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.lazyWorkerProvider = LazyWorkerProvider.of(() -> workerProviderFactory.captureAvailableWorkers(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes(),
                sessionVariable.getComputationFragmentSchedulingPolicy(), jobSpec.getComputeResource()));

        Map<PlanFragmentId, PlanFragment> fragmentMap =
                fragments.stream().collect(Collectors.toMap(PlanFragment::getFragmentId, Function.identity()));
        for (ScanNode scan : scanNodes) {
            PlanFragmentId id = scan.getFragmentId();
            PlanFragment fragment = fragmentMap.get(id);
            if (fragment == null) {
                // Fake a fragment for this node
                fragment = new PlanFragment(id, scan, DataPartition.RANDOM);
                executionDAG.attachFragments(Collections.singletonList(fragment));
            }
        }

        this.fragmentAssignmentStrategyFactory = new FragmentAssignmentStrategyFactory(connectContext, jobSpec, executionDAG);
    }

    public static TQueryGlobals genQueryGlobals(Instant startTime, String timezone) {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        String nowString = DATE_FORMAT.format(startTime.atZone(ZoneId.of(timezone)));
        queryGlobals.setNow_string(nowString);
        queryGlobals.setTimestamp_ms(startTime.toEpochMilli());
        queryGlobals.setTimestamp_us(startTime.getEpochSecond() * 1000000 + startTime.getNano() / 1000);
        if (timezone.equals("CST")) {
            queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            queryGlobals.setTime_zone(timezone);
        }
        return queryGlobals;
    }

    private WorkerProvider.Factory newWorkerProviderFactory() {
        if (RunMode.isSharedDataMode()) {
            return new DefaultSharedDataWorkerProvider.Factory();
        } else {
            return new DefaultWorkerProvider.Factory();
        }
    }

    public TUniqueId getQueryId() {
        return jobSpec.getQueryId();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    public TDescriptorTable getDescriptorTable() {
        return jobSpec.getDescTable();
    }

    public List<ExecutionFragment> getFragmentsInPreorder() {
        return executionDAG.getFragmentsInPreorder();
    }

    public TNetworkAddress getCoordAddress() {
        return coordAddress;
    }

    public TNetworkAddress getBrpcAddress(long workerId) {
        return getWorkerProvider().getWorkerById(workerId).getBrpcAddress();
    }

    public TNetworkAddress getBrpcIpAddress(long workerId) {
        return getWorkerProvider().getWorkerById(workerId).getBrpcIpAddress();
    }

    public TNetworkAddress getAddress(long workerId) {
        ComputeNode worker = getWorkerProvider().getWorkerById(workerId);
        return worker.getAddress();
    }

    /**
     * getWorkerProvider will trigger to load compute resource acquire, use LazyWorkerProvider instead if possible.
     */
    @Deprecated
    public WorkerProvider getWorkerProvider() {
        return lazyWorkerProvider.get();
    }

    public LazyWorkerProvider getLazyWorkerProvider() {
        return lazyWorkerProvider;
    }

    public TWorkGroup getResourceGroup() {
        return jobSpec.getResourceGroup();
    }

    public void prepareExec() throws StarRocksException {
        resetExec();
        computeFragmentInstances();
        traceInstance();
    }

    /**
     * Reset state of all the fragments set in Coordinator, when retrying the same query with the fragments.
     */
    private void resetExec() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        lazyWorkerProvider = LazyWorkerProvider.of(() -> workerProviderFactory.captureAvailableWorkers(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes(),
                sessionVariable.getComputationFragmentSchedulingPolicy(), jobSpec.getComputeResource()));

        jobSpec.getFragments().forEach(PlanFragment::reset);
    }

    private void traceInstance() {
        if (LOG.isDebugEnabled()) {
            // TODO(zc): add a switch to close this function
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            sb.append("query id=").append(DebugUtil.printId(jobSpec.getQueryId())).append(",");
            sb.append("fragment=[");
            for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(execFragment.getFragmentId());
                execFragment.appendTo(sb);
            }
            sb.append("]");
            LOG.debug(sb.toString());
        }
    }

    @VisibleForTesting
    FragmentScanRangeAssignment getFragmentScanRangeAssignment(PlanFragmentId fragmentId) {
        return executionDAG.getFragment(fragmentId).getScanRangeAssignment();
    }

    @VisibleForTesting
    void computeFragmentInstances() throws StarRocksException {
        maybeApplySmartDispatch();

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPostorder()) {
            fragmentAssignmentStrategyFactory.create(execFragment, lazyWorkerProvider.get()).assignFragmentToWorker(execFragment);
        }

        if (LOG.isDebugEnabled()) {
            executionDAG.getFragmentsInPreorder().forEach(
                    execFragment -> LOG.debug("fragment {} has instances {}", execFragment.getPlanFragment().getFragmentId(),
                            execFragment.getInstances().size()));
        }

        validateExecutionDAG();

        executionDAG.prepareCaptureVersion(enablePhasedSchedule);
        executionDAG.finalizeDAG();
    }

    private void maybeApplySmartDispatch() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (sessionVariable == null || !sessionVariable.isEnableSmartDispatch()) {
            return;
        }

        setupTrainingRecorder(sessionVariable);

        DispatchRequest dispatchRequest = null;
        try {
            WorkerProvider workerProvider = lazyWorkerProvider.get();
            dispatchRequest = buildDispatchRequest(workerProvider);

            executionDAG.setDispatchContexts(dispatchRequest.getFragmentContexts());
        } catch (Exception e) {
            LOG.warn("Failed to build dispatch request, training data will not be collected", e);
        }

        // Now run the actual dispatch algorithm
        if (dispatchRequest == null) {
            return;
        }

        try {
            CostEstimator.ModelPredictor predictor = buildModelPredictor(sessionVariable);
            CostEstimator costEstimator = predictor != null ? new CostEstimator(() -> predictor) : new CostEstimator();
            GreedyDispatchPolicy greedyPolicy = new GreedyDispatchPolicy(costEstimator,
                    sessionVariable.isEnableSmartDispatchShortTask());

            FragmentDispatcher dispatcher = new FragmentDispatcher(greedyPolicy);
            DispatchResult result = dispatcher.dispatch(dispatchRequest);

            // Store for recording after assignment completes
            this.lastBackendLoadStats = dispatchRequest.getBackendLoadStats();
            this.lastDispatchResult = result;

            executionDAG.setSmartDispatchResult(result);
        } catch (Exception e) {
            LOG.warn("Smart dispatcher failed, fallback to legacy fragment assignment", e);
        }
    }

    private void setupTrainingRecorder(SessionVariable sessionVariable) {
        String trainingPath = sessionVariable.getSmartDispatchTrainingPath();
        if (trainingPath == null || trainingPath.isEmpty()) {
            return;
        }
        try {
            executionDAG.setDispatchTrainingRecorder(new DispatchTrainingRecorder(Path.of(trainingPath)));
        } catch (Exception e) {
            LOG.warn("Failed to initialize DispatchTrainingRecorder, path={}", trainingPath, e);
        }
    }

    private DispatchRequest buildDispatchRequest(WorkerProvider workerProvider) {
        List<FragmentDispatchContext> fragmentContexts = new ArrayList<>();
        for (ExecutionFragment fragment : executionDAG.getFragmentsInPreorder()) {
            fragmentContexts.add(buildDispatchContext(fragment));
        }

        List<BackendLoadStat> backendLoadStats = buildBackendLoad(workerProvider);
        return new DispatchRequest(fragmentContexts, backendLoadStats);
    }

    private List<BackendLoadStat> buildBackendLoad(WorkerProvider workerProvider) {
        List<BackendLoadStat> backendLoadStats = new ArrayList<>();
        workerProvider.getAllWorkers().forEach(worker -> {
            double cpuPct = Math.min(1.0, Math.max(0.0, worker.getCpuUsedPermille() / 1000.0));
            double memPct = Math.min(1.0, Math.max(0.0, worker.getMemUsedPct()));
            int runningFragments = worker.getNumRunningQueries();
            backendLoadStats.add(new BackendLoadStat(
                    worker.getId(),
                    cpuPct,
                    memPct,
                    runningFragments,
                    0));
        });
        return backendLoadStats;
    }

    private FragmentDispatchContext buildDispatchContext(ExecutionFragment execFragment) {
        PlanFragment fragment = execFragment.getPlanFragment();
        PlanNode root = fragment.getPlanRoot();

        List<String> operatorTypes = new ArrayList<>();
        PlanStats planStats = new PlanStats();
        if (root != null) {
            collectPlanStats(root, 1, planStats, operatorTypes);
        }

        long estimatedScanRows = computeEstimatedScanRows(execFragment, root);
        double averageRowSize = computeAverageRowSize(execFragment, root);
        int pipelineDop = fragment.getPipelineDop();
        int requiredInstances = Math.max(1, fragment.getParallelExecNum());
        String query = getQueryString();
        long memLimit = jobSpec.getQueryOptions() != null ? jobSpec.getQueryOptions().getMem_limit() : 0L;
        int timeout = jobSpec.getQueryOptions() != null ? jobSpec.getQueryOptions().getQuery_timeout() : 0;

        // Determine fragment dispatch category and constraints
        FragmentDispatchContext.DispatchCategory category = determineDispatchCategory(execFragment);
        List<Long> constrainedWorkers = collectConstrainedWorkers(execFragment, category);

        return new FragmentDispatchContext(fragment.getFragmentId(),
                root != null ? root.getPlanNodeName() : "UNKNOWN",
                operatorTypes,
                estimatedScanRows,
                averageRowSize,
                pipelineDop,
                requiredInstances,
                query,
                planStats.joinCount,
                planStats.aggKeyCount,
                planStats.sortKeyCount,
                planStats.predicateCount,
                execFragment.getScanNodes().size(),
                Math.max(planStats.planHeight, 1),
                memLimit,
                timeout,
                category,
                constrainedWorkers);
    }

    /**
     * Determine the dispatch category for a fragment based on its characteristics.
     * This must align with FragmentAssignmentStrategyFactory's logic for consistency.
     */
    private FragmentDispatchContext.DispatchCategory determineDispatchCategory(ExecutionFragment execFragment) {
        PlanFragment fragment = execFragment.getPlanFragment();
        PlanNode leftMostNode = execFragment.getLeftMostNode();

        // Check if this is a scan fragment (leftmost node is a ScanNode)
        // This aligns with FragmentAssignmentStrategyFactory which uses LocalFragmentAssignmentStrategy
        // for fragments where leftMostNode is a ScanNode
        if (leftMostNode instanceof ScanNode) {
            return FragmentDispatchContext.DispatchCategory.SCAN;
        }

        // Check if this is a gather fragment (unpartitioned output)
        if (fragment.isGatherFragment()) {
            return FragmentDispatchContext.DispatchCategory.GATHER;
        }

        // Default: shuffle fragment that can be redistributed
        return FragmentDispatchContext.DispatchCategory.SHUFFLE;
    }

    /**
     * Collect the workers that this fragment is constrained to.
     * For SCAN fragments, this is determined by data locality (where the data resides).
     * For GATHER/SHUFFLE fragments, this is typically empty (no constraints).
     */
    private List<Long> collectConstrainedWorkers(ExecutionFragment execFragment,
                                                  FragmentDispatchContext.DispatchCategory category) {
        if (category == FragmentDispatchContext.DispatchCategory.SCAN) {
            // For SCAN fragments, the constrained workers are determined by the scan ranges
            // which tell us where the data resides. However, at this point we don't have
            // the scan range assignment yet. We need to use the child fragments' worker info
            // or rely on the scan range assignment computed during fragment assignment.
            //
            // Since scan range assignment happens later, we return empty here and let
            // the LocalFragmentAssignmentStrategy handle the actual worker assignment.
            // The key insight is: for SCAN fragments, smart dispatch should NOT override
            // the backend selection - it should only adjust DOP.
            //
            // We return empty list here to indicate "use original assignment"
            return Collections.emptyList();
        }

        // For GATHER and SHUFFLE fragments, no constraints by default
        return Collections.emptyList();
    }

    private int collectPlanStats(PlanNode node, int depth, PlanStats stats, List<String> operatorTypes) {
        operatorTypes.add(node.getPlanNodeName());
        if (node instanceof JoinNode) {
            stats.joinCount++;
        }
        if (node instanceof AggregationNode) {
            List<?> groupingExprs = ((AggregationNode) node).getAggInfo().getGroupingExprs();
            if (groupingExprs != null) {
                stats.aggKeyCount += groupingExprs.size();
            }
        }
        if (node instanceof SortNode) {
            stats.sortKeyCount += ((SortNode) node).getSortInfo().getOrderingExprs().size();
        }
        stats.predicateCount += node.getConjuncts().size();

        int maxDepth = depth;
        for (PlanNode child : node.getChildren()) {
            maxDepth = Math.max(maxDepth, collectPlanStats(child, depth + 1, stats, operatorTypes));
        }
        stats.planHeight = Math.max(stats.planHeight, maxDepth);
        return maxDepth;
    }

    private long computeEstimatedScanRows(ExecutionFragment execFragment, PlanNode root) {
        long fromScans = execFragment.getScanNodes().stream()
                .mapToLong(scan -> Math.max(0, scan.getCardinality()))
                .sum();
        if (fromScans > 0) {
            return fromScans;
        }
        if (root != null && root.getCardinality() > 0) {
            return root.getCardinality();
        }
        return 0L;
    }

    private double computeAverageRowSize(ExecutionFragment execFragment, PlanNode root) {
        double avg = execFragment.getScanNodes().stream()
                .mapToDouble(ScanNode::getAvgRowSize)
                .filter(v -> v > 0)
                .average()
                .orElse(Double.NaN);
        if (!Double.isNaN(avg)) {
            return avg;
        }
        if (root != null && root.getAvgRowSize() > 0) {
            return root.getAvgRowSize();
        }
        return 0.0;
    }

    private CostEstimator.ModelPredictor buildModelPredictor(SessionVariable sessionVariable) {
        if (!sessionVariable.isEnableSmartDispatchModel()) {
            return null;
        }
        String cpuModel = sessionVariable.getSmartDispatchCpuModelPath();
        String memModel = sessionVariable.getSmartDispatchMemModelPath();
        String timeModel = sessionVariable.getSmartDispatchTimeModelPath();
        if (StringUtils.isEmpty(cpuModel) || StringUtils.isEmpty(memModel) || StringUtils.isEmpty(timeModel)) {
            LOG.warn("Smart dispatch model is enabled but model paths are empty, fallback to heuristic");
            return null;
        }
        String pythonExec = sessionVariable.getSmartDispatchPythonExec();
        String script = new java.io.File("fe-core/scripts/fragment_cost_model.py").getPath();
        return new PythonModelPredictor(pythonExec, script, cpuModel, memModel, timeModel);
    }

    private String getQueryString() {
        if (connectContext == null || connectContext.getExecutor() == null) {
            return "";
        }
        try {
            return connectContext.getExecutor().getOriginStmtInString();
        } catch (Exception e) {
            return "";
        }
    }

    private static class PlanStats {
        int joinCount;
        int aggKeyCount;
        int sortKeyCount;
        int predicateCount;
        int planHeight;
    }

    public void assignIncrementalScanRangesToFragmentInstances(ExecutionFragment execFragment) throws
            StarRocksException {
        execFragment.getScanRangeAssignment().clear();
        for (FragmentInstance instance : execFragment.getInstances()) {
            instance.resetAllScanRanges();
        }
        fragmentAssignmentStrategyFactory.create(execFragment, lazyWorkerProvider.get()).assignFragmentToWorker(execFragment);
    }

    private void validateExecutionDAG() throws StarRocksPlannerException {
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            DataSink sink = execFragment.getPlanFragment().getSink();
            if (sink instanceof ResultSink && execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException("This sql plan has multi result sinks", ErrorType.INTERNAL_ERROR);
            }

            if (sink instanceof TableFunctionTableSink && (((TableFunctionTableSink) sink).isWriteSingleFile())
                    && execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException(
                        "This sql plan has multi table function table sinks, but set to write single file",
                        ErrorType.INTERNAL_ERROR);
            }
        }
    }

    public TFragmentInstanceFactory createTFragmentInstanceFactory() {
        return new TFragmentInstanceFactory(connectContext, jobSpec, executionDAG, coordAddress);
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEHTTP();
        }
        return null;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEPort();
        }
        return null;
    }

    public static TWorkGroup prepareResourceGroup(ConnectContext connect, ResourceGroupClassifier.QueryType queryType) {
        if (connect == null || !connect.getSessionVariable().isEnableResourceGroup()) {
            return null;
        }

        final ResourceGroupMgr resourceGroupMgr = GlobalStateMgr.getCurrentState().getResourceGroupMgr();
        final SessionVariable sessionVariable = connect.getSessionVariable();
        TWorkGroup resourceGroup = null;

        // 1. try to use the resource group specified by the variable
        if (StringUtils.isNotEmpty(sessionVariable.getResourceGroup())) {
            String rgName = sessionVariable.getResourceGroup();
            resourceGroup = resourceGroupMgr.chooseResourceGroupByName(rgName);
        }

        // 2. try to use the resource group specified by workgroup_id
        long workgroupId = connect.getSessionVariable().getResourceGroupId();
        if (resourceGroup == null && workgroupId > 0) {
            resourceGroup = resourceGroupMgr.chooseResourceGroupByID(workgroupId);
        }

        // 3. if the specified resource group not exist try to use the default one
        if (resourceGroup == null) {
            Set<Long> dbIds = connect.getCurrentSqlDbIds();
            resourceGroup = resourceGroupMgr.chooseResourceGroup(connect, queryType, dbIds);
        }

        if (resourceGroup == null) {
            resourceGroup = resourceGroupMgr.chooseResourceGroupByName(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
        }

        if (resourceGroup != null) {
            connect.getAuditEventBuilder().setResourceGroup(resourceGroup.getName());
            connect.setResourceGroup(resourceGroup);
        } else {
            connect.getAuditEventBuilder().setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
        }

        return resourceGroup;
    }

}
