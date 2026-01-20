package com.starrocks.qe.scheduler.dispatch;

import com.google.common.base.MoreObjects;

public class FragmentCost {

    public static final double SHORT_FRAGMENT_THRESHOLD_MS = 10.0;

    private final double predictedCpuUsagePercent;
    private final double predictedMemBytes;
    private final double predictedMaxTimeMs;

    public FragmentCost(double predictedCpuUsagePercent, double predictedMemBytes, double predictedMaxTimeMs) {
        this.predictedCpuUsagePercent = predictedCpuUsagePercent;
        this.predictedMemBytes = predictedMemBytes;
        this.predictedMaxTimeMs = predictedMaxTimeMs;
    }

    public double getPredictedCpuUsagePercent() {
        return predictedCpuUsagePercent;
    }

    public double getPredictedMemBytes() {
        return predictedMemBytes;
    }

    public double getPredictedMaxTimeMs() {
        return predictedMaxTimeMs;
    }

    public boolean isShortFragment() {
        return predictedMaxTimeMs < SHORT_FRAGMENT_THRESHOLD_MS;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("predictedCpuUsagePercent", predictedCpuUsagePercent)
                .add("predictedMemBytes", predictedMemBytes)
                .add("predictedMaxTimeMs", predictedMaxTimeMs)
                .add("isShortFragment", isShortFragment())
                .toString();
    }
}
