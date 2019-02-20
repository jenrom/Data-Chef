package de.areto.common.concurrent;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;

public class PriorityExecutorTest {

    private static int completed = 0;

    private static synchronized void incComleted() {
        completed++;
    }

    public static class LengthyJob implements PriorityCallable<String> {

        private final String value;
        private final long priority;

        public LengthyJob(String value, long priority) {
            this.value = value;
            this.priority = priority;
        }

        public LengthyJob(String value) {
            this(value, System.nanoTime());
        }

        @Override
        public long getPriority() {
            return priority;
        }

        @Override
        public String call() {
            System.out.println("Exec: " + this);
            long num = 1000000;
            for (int i = 0; i < 1000000; i++) {
                num *= Math.random() * 1000;
                num /= Math.random() * 1000;
                if (num == 0)
                    num = 1000000;
            }

            incComleted();

            return value;
        }

        @Override
        public String toString() {
            return "LengthyJob{" + value + ", " + priority + "}";
        }
    }

    @Test
    public void testPriorityExecutor() {
        final int jobs = 10;
        final ExecutorService exec = PriorityExecutors.newSingleThreadExecutor();

        List<LengthyJob> jobList = new ArrayList<>(10);

        for (int i = 0; i < jobs; i++) {
            final String desc = "Job " + i;
            final LengthyJob job = i==3 ? new LengthyJob(desc, -1) : new LengthyJob(desc);
            jobList.add(job);
        }

        for(LengthyJob job : jobList) {
            System.out.println("Submitting: " + job);
            exec.submit(job);
        }

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(completed).isEqualTo(jobs));

    }
}
