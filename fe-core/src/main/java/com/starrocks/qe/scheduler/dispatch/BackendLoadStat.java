package com.starrocks.qe.scheduler.dispatch;

import com.google.common.base.MoreObjects;

public class BackendLoadStat {
    private static final double DEFAULT_RUNNING_FRAGMENT_LIMIT = 64.0d;

    private final long beId;
    private final double cpuUsagePercent;
    private final double memUsagePercent;
    private final int runningFragmentCount;
    private final int recentFailureCount;

    public BackendLoadStat(long beId,
                           double cpuUsagePercent,
                           double memUsagePercent,
                           int runningFragmentCount,
                           int recentFailureCount) {
        this.beId = beId;
        this.cpuUsagePercent = cpuUsagePercent;
        this.memUsagePercent = memUsagePercent;
        this.runningFragmentCount = runningFragmentCount;
        this.recentFailureCount = recentFailureCount;
    }

    public long getBeId() {
        return beId;
    }

    public double getCpuUsagePercent() {
        return cpuUsagePercent;
    }

    public double getMemUsagePercent() {
        return memUsagePercent;
    }

    public int getRunningFragmentCount() {
        return runningFragmentCount;
    }

    public int getRecentFailureCount() {
        return recentFailureCount;
    }

    public double getLoadScore() {
        double normalizedConcurrency = runningFragmentCount / DEFAULT_RUNNING_FRAGMENT_LIMIT;
        return (0.4 * cpuUsagePercent) + (0.3 * memUsagePercent)
                + (0.2 * normalizedConcurrency) + (0.1 * recentFailureCount);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("beId", beId)
                .add("cpuUsagePercent", cpuUsagePercent)
                .add("memUsagePercent", memUsagePercent)
                .add("runningFragmentCount", runningFragmentCount)
                .add("recentFailureCount", recentFailureCount)
                .add("score", getLoadScore())
                .toString();
    }
}
