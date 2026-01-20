package com.starrocks.qe.scheduler.dispatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DispatchTrainingRecorder implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(DispatchTrainingRecorder.class);
    private static final String HEADER =
            "fragment_id,fragment_type,scan_rows,avg_row_size,dop," +
            "join_count,agg_count,sort_count,plan_height," +
            "instance_count,total_cpu_ns,max_cpu_ns,avg_cpu_ns," +
            "total_time_ms,max_time_ms,avg_time_ms," +
            "total_mem_bytes,max_mem_bytes,avg_mem_bytes,cpu_usage_percent";

    private final Path csvPath;
    private final ExecutorService executor;
    private final AtomicBoolean headerWritten;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public DispatchTrainingRecorder(Path csvPath) {
        this(csvPath, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "dispatch-training-recorder");
            t.setDaemon(true);
            return t;
        }));
    }

    DispatchTrainingRecorder(Path csvPath, ExecutorService executor) {
        this.csvPath = Objects.requireNonNull(csvPath, "csvPath is null");
        this.executor = Objects.requireNonNull(executor, "executor is null");
        boolean hasData = false;
        try {
            hasData = Files.exists(csvPath) && Files.size(csvPath) > 0;
        } catch (IOException e) {
            LOG.warn("Failed to probe training CSV {}, will rewrite header on first record", csvPath, e);
        }
        this.headerWritten = new AtomicBoolean(hasData);
    }

    public void record(FragmentDispatchContext context, FragmentInstanceStats stats) {
        if (closed.get()) {
            LOG.debug("Skip recording because recorder is closed");
            return;
        }
        Objects.requireNonNull(context, "context is null");
        Objects.requireNonNull(stats, "stats is null");
        executor.submit(() -> appendRecord(context, stats));
    }

    private void appendRecord(FragmentDispatchContext context, FragmentInstanceStats stats) {
        try {
            if (csvPath.getParent() != null) {
                Files.createDirectories(csvPath.getParent());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(
                    csvPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (headerWritten.compareAndSet(false, true)) {
                    writer.write(HEADER);
                    writer.newLine();
                }
                writer.write(buildRow(context, stats));
                writer.newLine();
            }
        } catch (IOException e) {
            LOG.warn("Failed to append dispatch training record for fragment {}", context.getFragmentId(), e);
        }
    }

    private String buildRow(FragmentDispatchContext context, FragmentInstanceStats stats) {
        int dop = Math.max(1, context.getPipelineDop());
        int instanceCount = stats.getCompletedCount();

        double cpuUsagePercent = 0.0;
        long maxTimeMs = stats.getMaxTimeMs();
        long totalCpuNs = stats.getTotalCpuNs();
        if (maxTimeMs > 0 && totalCpuNs > 0 && instanceCount > 0) {
            long totalAvailableCpuNs = maxTimeMs * 1_000_000L * dop * instanceCount;
            cpuUsagePercent = (double) totalCpuNs / totalAvailableCpuNs * 100.0;
            cpuUsagePercent = Math.max(0.0, Math.min(cpuUsagePercent, 100.0));
        }

        StringBuilder sb = new StringBuilder(512);
        sb.append(context.getFragmentId()).append(',')
                .append(context.getFragmentType()).append(',')
                .append(context.getEstimatedScanRows()).append(',')
                .append(context.getAverageRowSize()).append(',')
                .append(dop).append(',')
                .append(context.getJoinCount()).append(',')
                .append(context.getAggKeyCount()).append(',')
                .append(context.getSortKeyCount()).append(',')
                .append(context.getPlanHeight()).append(',')
                .append(instanceCount).append(',')
                .append(stats.getTotalCpuNs()).append(',')
                .append(stats.getMaxCpuNs()).append(',')
                .append(stats.getAvgCpuNs()).append(',')
                .append(stats.getTotalTimeMs()).append(',')
                .append(stats.getMaxTimeMs()).append(',')
                .append(stats.getAvgTimeMs()).append(',')
                .append(stats.getTotalMemBytes()).append(',')
                .append(stats.getMaxMemBytes()).append(',')
                .append(stats.getAvgMemBytes()).append(',')
                .append(String.format("%.2f", cpuUsagePercent));
        return sb.toString();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
