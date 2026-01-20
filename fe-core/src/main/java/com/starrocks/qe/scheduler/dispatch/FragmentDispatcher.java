package com.starrocks.qe.scheduler.dispatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

public class FragmentDispatcher {
    private static final Logger LOG = LogManager.getLogger(FragmentDispatcher.class);

    private final DispatchPolicy dispatchPolicy;

    public FragmentDispatcher(DispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = Objects.requireNonNull(dispatchPolicy, "dispatchPolicy is null");
    }

    public DispatchResult dispatch(List<FragmentDispatchContext> contexts, List<BackendLoadStat> backendLoadStats) {
        return dispatch(new DispatchRequest(contexts, backendLoadStats));
    }

    public DispatchResult dispatch(DispatchRequest request) {
        DispatchResult result = dispatchPolicy.dispatch(request);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatch result: {}", result);
        }
        return result;
    }
}
