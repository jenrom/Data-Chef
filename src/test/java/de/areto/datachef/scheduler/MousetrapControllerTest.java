package de.areto.datachef.scheduler;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class MousetrapControllerTest {

    @Test
    public void timeoutShouldOccur() throws InterruptedException {
        final MousetrapController c1 = new MousetrapController((m) -> {
            fail("Mapping should not be triggered");
            return null;
        });

        final HashSet<String> requiredNames = Sets.newHashSet("a", "b");
        final int timeoutSeconds = 1;
        final Duration timeout = Duration.ofSeconds(timeoutSeconds);

        final Mousetrap trap = c1.createTrap("test", requiredNames, timeout);

        c1.notify("a");

        TimeUnit.SECONDS.sleep(timeoutSeconds + 1);

        assertThat(trap.isTimedOut()).isTrue();
        assertThat(trap.canStrike()).isFalse();
    }

    @Test
    public void timeoutShouldNotOccur() throws InterruptedException {
        final MousetrapController c1 = new MousetrapController((m) -> {
            fail("Mapping should not be triggered");
            return null;
        });

        final HashSet<String> requiredNames = Sets.newHashSet("a", "b");
        final int timeoutSeconds = 1;
        final Duration timeout = Duration.ofSeconds(timeoutSeconds);

        final Mousetrap trap = c1.createTrap("test", requiredNames, timeout);

        TimeUnit.SECONDS.sleep(timeoutSeconds + 1);

        assertThat(trap.isTimedOut()).isFalse();
        assertThat(trap.canStrike()).isFalse();
    }

    @Test
    public void trapControllerShouldReactAsExpected() {
        final MousetrapController c1 = new MousetrapController((m) -> {
            assertThat(m).isEqualTo("test");
            return null;
        });

        final HashSet<String> requiredNames = Sets.newHashSet("a", "b");
        final Mousetrap trap = c1.createTrap("test", requiredNames, Duration.ofSeconds(30));

        c1.notify("a");
        c1.notify("b");

        assertThat(trap.isEmpty()).isTrue();
    }

    @Test
    public void trapShouldReactAsExpected() {
        final MousetrapController controller = new MousetrapController((m) -> null);

        final HashSet<String> requiredMappings = Sets.newHashSet("a", "b");
        final Duration timeoutDuration = Duration.ofSeconds(30);
        final Mousetrap trap = controller.createTrap("test", requiredMappings, timeoutDuration);

        assertThat(controller.contains("test")).isTrue();

        assertThat(trap.isTimedOut()).isFalse();
        assertThat(trap.canStrike()).isFalse();

        assertThat(trap.wasMappingNameReceived("a")).isFalse();
        assertThat(trap.wasMappingNameReceived("b")).isFalse();

        assertThat(trap.isMappingNameRequired("a")).isTrue();
        assertThat(trap.isMappingNameRequired("b")).isTrue();

        trap.offerMappingName("a");
        trap.offerMappingName("b");

        assertThat(trap.wasMappingNameReceived("a")).isTrue();
        assertThat(trap.wasMappingNameReceived("b")).isTrue();
        assertThat(trap.canStrike()).isTrue();

        trap.reset();

        assertThat(trap.isTimedOut()).isFalse();
        assertThat(trap.canStrike()).isFalse();

        assertThat(trap.wasMappingNameReceived("a")).isFalse();
        assertThat(trap.wasMappingNameReceived("b")).isFalse();

        assertThat(trap.isMappingNameRequired("a")).isTrue();
        assertThat(trap.isMappingNameRequired("b")).isTrue();
    }
}