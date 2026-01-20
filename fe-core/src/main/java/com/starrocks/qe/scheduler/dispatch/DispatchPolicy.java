package com.starrocks.qe.scheduler.dispatch;

public interface DispatchPolicy {
    DispatchResult dispatch(DispatchRequest request);
}
