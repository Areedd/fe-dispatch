package com.starrocks.qe.scheduler.dispatch;

import com.google.common.base.MoreObjects;
import com.starrocks.planner.PlanFragmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DispatchResult {
    private final Map<PlanFragmentId, List<Long>> fragmentToBackends;

    public DispatchResult(Map<PlanFragmentId, List<Long>> fragmentToBackends) {
        Map<PlanFragmentId, List<Long>> copiedMap = new HashMap<>();
        for (Map.Entry<PlanFragmentId, List<Long>> entry :
                Objects.requireNonNull(fragmentToBackends, "fragmentToBackends is null").entrySet()) {
            copiedMap.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        this.fragmentToBackends = Collections.unmodifiableMap(copiedMap);
    }

    public Map<PlanFragmentId, List<Long>> getFragmentToBackends() {
        return fragmentToBackends;
    }

    public Long getPrimaryBackend(PlanFragmentId fragmentId) {
        List<Long> backends = fragmentToBackends.get(fragmentId);
        return (backends != null && !backends.isEmpty()) ? backends.get(0) : null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("fragmentToBackends", fragmentToBackends)
                .toString();
    }
}
