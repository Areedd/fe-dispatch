package com.starrocks.qe.scheduler.dispatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PythonModelPredictor implements CostEstimator.ModelPredictor {
    private static final Logger LOG = LogManager.getLogger(PythonModelPredictor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long DEFAULT_TIMEOUT_MS = 5_000L;

    private final String pythonExec;
    private final String scriptPath;
    private final String cpuModelPath;
    private final String memModelPath;
    private final String timeModelPath;

    private Process serverProcess;
    private BufferedWriter serverStdin;
    private BufferedReader serverStdout;

    public PythonModelPredictor(String pythonExec, String scriptPath, String cpuModelPath,
                                 String memModelPath, String timeModelPath) {
        this.pythonExec = pythonExec;
        this.scriptPath = scriptPath;
        this.cpuModelPath = cpuModelPath;
        this.memModelPath = memModelPath;
        this.timeModelPath = timeModelPath;
        startServer();
    }

    @Override
    public synchronized FragmentCost predict(FragmentDispatchContext context, double[] features) throws Exception {
        // Try server mode first; fall back to one-shot process if needed.
        if (serverProcess != null && serverProcess.isAlive()) {
            try {
                return predictViaServer(context);
            } catch (Exception e) {
                LOG.warn("Python predictor server mode failed, fallback to one-shot", e);
                stopServer();
            }
        }
        ProcessBuilder builder = new ProcessBuilder(
                pythonExec,
                scriptPath,
                "--cpu-model", cpuModelPath,
                "--mem-model", memModelPath,
                "--time-model", timeModelPath,
                "--input-json", buildFeatureJson(context)
        );
        builder.redirectErrorStream(true);
        Process process = builder.start();
        boolean finished = process.waitFor(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("Python predictor timed out");
        }
        if (process.exitValue() != 0) {
            String errorOutput = readAll(process);
            throw new IOException("Python predictor failed: " + errorOutput);
        }
        String output = readAll(process);
        return parseResult(output);
    }

    private FragmentCost predictViaServer(FragmentDispatchContext context) throws Exception {
        String payload = buildFeatureJson(context);
        serverStdin.write(payload);
        serverStdin.write("\n");
        serverStdin.flush();
        String line = serverStdout.readLine();
        if (line == null) {
            throw new IOException("Python predictor server returned null");
        }
        return parseResult(line);
    }

    private void startServer() {
        try {
            ProcessBuilder builder = new ProcessBuilder(
                    pythonExec,
                    scriptPath,
                    "--server",
                    "--cpu-model", cpuModelPath,
                    "--mem-model", memModelPath,
                    "--time-model", timeModelPath);
            builder.redirectErrorStream(true);
            serverProcess = builder.start();
            serverStdin = new BufferedWriter(new OutputStreamWriter(serverProcess.getOutputStream(), StandardCharsets.UTF_8));
            serverStdout = new BufferedReader(new InputStreamReader(serverProcess.getInputStream(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.warn("Failed to start Python predictor server process, will use one-shot mode", e);
            stopServer();
        }
    }

    private void stopServer() {
        if (serverProcess != null) {
            serverProcess.destroyForcibly();
            serverProcess = null;
        }
        serverStdin = null;
        serverStdout = null;
    }

    private String buildFeatureJson(FragmentDispatchContext context) throws IOException {
        Map<String, Object> row = new HashMap<>();
        row.put("fragment_type", context.getFragmentType());
        row.put("scan_rows", context.getEstimatedScanRows());
        row.put("avg_row_size", context.getAverageRowSize());
        row.put("dop", context.getPipelineDop());
        row.put("join_count", context.getJoinCount());
        row.put("agg_count", context.getAggKeyCount());
        row.put("sort_count", context.getSortKeyCount());
        row.put("plan_height", context.getPlanHeight());
        String json = MAPPER.writeValueAsString(new Map[]{row});
        return json;
    }

    private FragmentCost parseResult(String json) throws IOException {
        JsonNode root = MAPPER.readTree(json);
        if (!root.isArray() || root.size() == 0) {
            throw new IOException("Python predictor returned empty result");
        }
        JsonNode first = root.get(0);
        double cpuPercent = Math.max(0, Math.min(100, first.path("predictedCpuUsagePercent").asDouble(Double.NaN)));
        double mem = Math.max(0, first.path("predictedMemBytes").asDouble(Double.NaN));
        double timeMs = Math.max(0, first.path("predictedMaxTimeMs").asDouble(Double.NaN));
        if (Double.isNaN(cpuPercent) || Double.isNaN(mem) || Double.isNaN(timeMs)) {
            throw new IOException("Python predictor returned invalid values: " + json);
        }
        return new FragmentCost(cpuPercent, mem, timeMs);
    }

    private String readAll(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    @VisibleForTesting
    String getPythonExec() {
        return pythonExec;
    }
}
