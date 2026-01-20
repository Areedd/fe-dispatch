package com.starrocks.qe.scheduler.dispatch;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FragmentInstanceStats {
    private final int expectedInstanceCount;
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final AtomicLong totalCpuNs = new AtomicLong(0);
    private final AtomicLong maxCpuNs = new AtomicLong(0);
    private final AtomicLong totalTimeMs = new AtomicLong(0);
    private final AtomicLong maxTimeMs = new AtomicLong(0);
    private final AtomicLong totalMemBytes = new AtomicLong(0);
    private final AtomicLong maxMemBytes = new AtomicLong(0);

    public FragmentInstanceStats(int expectedInstanceCount) {
        this.expectedInstanceCount = Math.max(1, expectedInstanceCount);
    }

    public boolean addInstanceStats(long cpuNs, long timeMs, long memBytes) {
        totalCpuNs.addAndGet(cpuNs);
        totalTimeMs.addAndGet(timeMs);
        totalMemBytes.addAndGet(memBytes);

        updateMax(maxCpuNs, cpuNs);
        updateMax(maxTimeMs, timeMs);
        updateMax(maxMemBytes, memBytes);

        return completedCount.incrementAndGet() >= expectedInstanceCount;
    }

    private void updateMax(AtomicLong max, long value) {
        long current;
        do {
            current = max.get();
            if (value <= current) {
                return;
            }
        } while (!max.compareAndSet(current, value));
    }

    public int getExpectedInstanceCount() {
        return expectedInstanceCount;
    }

    public int getCompletedCount() {
        return completedCount.get();
    }

    public long getTotalCpuNs() {
        return totalCpuNs.get();
    }

    public long getMaxCpuNs() {
        return maxCpuNs.get();
    }

    public long getTotalTimeMs() {
        return totalTimeMs.get();
    }

    public long getMaxTimeMs() {
        return maxTimeMs.get();
    }

    public long getTotalMemBytes() {
        return totalMemBytes.get();
    }

    public long getMaxMemBytes() {
        return maxMemBytes.get();
    }

    public long getAvgCpuNs() {
        int count = completedCount.get();
        return count > 0 ? totalCpuNs.get() / count : 0;
    }

    public long getAvgTimeMs() {
        int count = completedCount.get();
        return count > 0 ? totalTimeMs.get() / count : 0;
    }

    public long getAvgMemBytes() {
        int count = completedCount.get();
        return count > 0 ? totalMemBytes.get() / count : 0;
    }

    public boolean isComplete() {
        return completedCount.get() >= expectedInstanceCount;
    }
}
