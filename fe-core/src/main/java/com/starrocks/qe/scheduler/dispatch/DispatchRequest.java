package com.starrocks.qe.scheduler.dispatch;

import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DispatchRequest {
    private final List<FragmentDispatchContext> fragmentContexts;
    private final List<BackendLoadStat> backendLoadStats;

    public DispatchRequest(List<FragmentDispatchContext> fragmentContexts,
                           List<BackendLoadStat> backendLoadStats) {
        this.fragmentContexts = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(fragmentContexts, "fragmentContexts is null")));
        this.backendLoadStats = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(backendLoadStats, "backendLoadStats is null")));
    }

    public List<FragmentDispatchContext> getFragmentContexts() {
        return fragmentContexts;
    }

    public List<BackendLoadStat> getBackendLoadStats() {
        return backendLoadStats;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("fragmentContexts", fragmentContexts)
                .add("backendLoadStats", backendLoadStats)
                .toString();
    }
}
