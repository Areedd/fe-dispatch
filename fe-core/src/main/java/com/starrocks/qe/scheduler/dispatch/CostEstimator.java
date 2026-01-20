package com.starrocks.qe.scheduler.dispatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

public class CostEstimator {
    private static final Logger LOG = LogManager.getLogger(CostEstimator.class);

    private final Supplier<ModelPredictor> predictorSupplier;

    public CostEstimator() {
        this(null);
    }

    public CostEstimator(Supplier<ModelPredictor> predictorSupplier) {
        this.predictorSupplier = predictorSupplier;
    }

    public FragmentCost estimate(FragmentDispatchContext context) {
        Objects.requireNonNull(context, "context is null");
        double[] features = buildFeatures(context);
        ModelPredictor predictor = predictorSupplier == null ? null : predictorSupplier.get();
        if (predictor != null) {
            try {
                FragmentCost cost = predictor.predict(context, features);
                if (cost != null) {
                    return cost;
                }
            } catch (Exception e) {
                LOG.warn("Model prediction failed for fragment {} with features {}", context.getFragmentId(),
                        Arrays.toString(features), e);
            }
        }
        return new FragmentCost(0.0, 0.0, Double.MAX_VALUE);
    }


    private double[] buildFeatures(FragmentDispatchContext context) {
        double fragmentTypeHash = Math.abs(context.getFragmentType().hashCode() % 10_000);
        return new double[]{
                context.getEstimatedScanRows(),
                context.getAverageRowSize(),
                context.getPipelineDop(),
                context.getJoinCount(),
                context.getAggKeyCount(),
                context.getSortKeyCount(),
                context.getPlanHeight(),
                fragmentTypeHash
        };
    }

    public interface ModelPredictor {
        FragmentCost predict(FragmentDispatchContext context, double[] features) throws Exception;
    }
}
