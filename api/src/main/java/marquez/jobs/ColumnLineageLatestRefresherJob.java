/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.jobs;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;

/** A job that refreshes the temporary column lineage latest table on a fixed schedule in Marquez. */
@Slf4j
public class ColumnLineageLatestRefresherJob extends AbstractScheduledService implements Managed {

  private static final int FREQUENCY_MINS = 30;
  private final Scheduler fixedRateScheduler;
  private final Jdbi jdbi;

  public ColumnLineageLatestRefresherJob(@NonNull final Jdbi jdbi) {
    this.jdbi = jdbi;
    // Define fixed schedule with no initial delay
    this.fixedRateScheduler = Scheduler.newFixedRateSchedule(
        Duration.ZERO, Duration.ofMinutes(FREQUENCY_MINS));
  }

  @Override
  protected Scheduler scheduler() {
    return fixedRateScheduler;
  }

  @Override
  public void start() throws Exception {
    startAsync().awaitRunning();
    log.info("Refreshing tmp_column_lineage_latest table every '{}' mins.", FREQUENCY_MINS);
  }

  @Override
  public void stop() throws Exception {
    stopAsync().awaitTerminated();
    log.info("Stopped refreshing tmp_column_lineage_latest table.");
  }

  @Override
  protected void runOneIteration() {
    try {
      log.info("Refreshing tmp_column_lineage_latest table...");
      jdbi.useHandle(handle -> {
        handle.execute("SELECT refresh_tmp_column_lineage_latest()");
      });
      log.info("Table tmp_column_lineage_latest refreshed successfully.");
    } catch (Exception error) {
      log.error("Failed to refresh tmp_column_lineage_latest table. Retrying on next run...", error);
    }
  }
} 