package org.ndsu.agda.connect.utils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.common.utils.Time;

public class SourceRecordDequeBuilder {
    private SourceRecordDequeBuilder() {

    }

    Time time = Time.SYSTEM;
    private int maximumCapacity = Integer.MAX_VALUE;
    private int batchSize = 1024;
    private int emptyWaitMs = 0;
    private int maximumCapacityWaitMs = 100;
    private int maximumCapacityTimeoutMs = 60000;
    private RateLimiter writeRateLimiter = null;

    public int maximumCapacity() {
        return this.maximumCapacity;
    }

    public SourceRecordDequeBuilder maximumCapacity(int maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
        return this;
    }

    public int batchSize() {
        return this.batchSize;
    }

    public SourceRecordDequeBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int emptyWaitMs() {
        return this.emptyWaitMs;
    }

    public SourceRecordDequeBuilder emptyWaitMs(int emptyWaitMs) {
        this.emptyWaitMs = emptyWaitMs;
        return this;
    }

    public int maximumCapacityWaitMs() {
        return this.maximumCapacityWaitMs;
    }

    public SourceRecordDequeBuilder maximumCapacityWaitMs(int maximumCapacityWaitMs) {
        this.maximumCapacityWaitMs = maximumCapacityWaitMs;
        return this;
    }

    public int maximumCapacityTimeoutMs() {
        return this.maximumCapacityTimeoutMs;
    }

    public SourceRecordDequeBuilder maximumCapacityTimeoutMs(int maximumCapacityTimeoutMs) {
        this.maximumCapacityTimeoutMs = maximumCapacityTimeoutMs;
        return this;
    }

    public RateLimiter writeRateLimiter() {
        return this.writeRateLimiter;
    }

    public SourceRecordDequeBuilder writeRateLimiter(RateLimiter writeRateLimiter) {
        this.writeRateLimiter = writeRateLimiter;
        return this;
    }

    public static final SourceRecordDequeBuilder of() {
        return new SourceRecordDequeBuilder();
    }

    public SourceRecordDeque build() {
        Preconditions.checkArgument(maximumCapacity > 0, "maximumCapacity must be greater than zero.");
        Preconditions.checkArgument(emptyWaitMs >= 0, "emptyWaitMs must be greater than or equal to zero.");
        Preconditions.checkArgument(maximumCapacityWaitMs > 0, "maximumCapacityWaitMs must be greater than zero.");
        Preconditions.checkArgument(maximumCapacityTimeoutMs > 0, "maximumCapacityTimeoutMs must be greater than zero.");

        return new SourceRecordDequeImpl(
                this.time,
                this.maximumCapacity,
                this.batchSize,
                this.emptyWaitMs,
                this.maximumCapacityWaitMs,
                this.maximumCapacityTimeoutMs,
                this.writeRateLimiter);
    }

}