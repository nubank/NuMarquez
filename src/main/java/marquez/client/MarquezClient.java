/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.client;

import static com.google.common.base.Preconditions.checkArgument;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import marquez.client.models.Dataset;
import marquez.client.models.DatasetMeta;
import marquez.client.models.Job;
import marquez.client.models.JobMeta;
import marquez.client.models.Namespace;
import marquez.client.models.NamespaceMeta;
import marquez.client.models.Run;
import marquez.client.models.RunMeta;
import marquez.client.models.RunState;
import marquez.client.models.Source;
import marquez.client.models.SourceMeta;
import marquez.client.models.Tag;

@Slf4j
public class MarquezClient {
  @VisibleForTesting
  static final URL DEFAULT_BASE_URL = Utils.toUrl("http://localhost:8080/api/v1");

  @VisibleForTesting static final int DEFAULT_LIMIT = 100;
  @VisibleForTesting static final int DEFAULT_OFFSET = 0;

  @VisibleForTesting final MarquezHttp http;

  public MarquezClient() {
    this(DEFAULT_BASE_URL);
  }

  public MarquezClient(final String baseUrlString) {
    this(Utils.toUrl(baseUrlString));
  }

  public MarquezClient(final URL baseUrl) {
    this(MarquezHttp.create(baseUrl, MarquezClient.Version.get()));
  }

  MarquezClient(@NonNull final MarquezHttp http) {
    this.http = http;
  }

  public Namespace createNamespace(
      @NonNull String namespaceName, @NonNull NamespaceMeta namespaceMeta) {
    final String bodyAsJson =
        http.put(http.url("/namespaces/%s", namespaceName), namespaceMeta.toJson());
    return Namespace.fromJson(bodyAsJson);
  }

  public Namespace getNamespace(@NonNull String namespaceName) {
    final String bodyAsJson = http.get(http.url("/namespaces/%s", namespaceName));
    return Namespace.fromJson(bodyAsJson);
  }

  public List<Namespace> listNamespaces() {
    return listNamespaces(DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public List<Namespace> listNamespaces(int limit, int offset) {
    final String bodyAsJson = http.get(http.url("/namespaces", newQueryParamsWith(limit, offset)));
    return Namespaces.fromJson(bodyAsJson).getValue();
  }

  public Source createSource(@NonNull String sourceName, @NonNull SourceMeta sourceMeta) {
    final String bodyAsJson = http.put(http.url("/sources/%s", sourceName), sourceMeta.toJson());
    return Source.fromJson(bodyAsJson);
  }

  public Source getSource(@NonNull String sourceName) {
    final String bodyAsJson = http.get(http.url("/sources/%s", sourceName));
    return Source.fromJson(bodyAsJson);
  }

  public List<Source> listSources(String namespaceName) {
    return listSources(namespaceName, DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public List<Source> listSources(@NonNull String namespaceName, int limit, int offset) {
    final String bodyAsJson = http.get(http.url("/sources", newQueryParamsWith(limit, offset)));
    return Sources.fromJson(bodyAsJson).getValue();
  }

  public Dataset createDataset(
      @NonNull String namespaceName,
      @NonNull String datasetName,
      @NonNull DatasetMeta datasetMeta) {
    final String bodyAsJson =
        http.put(
            http.url("/namespaces/%s/datasets/%s", namespaceName, datasetName),
            datasetMeta.toJson());
    return Dataset.fromJson(bodyAsJson);
  }

  public Dataset getDataset(@NonNull String namespaceName, @NonNull String datasetName) {
    final String bodyAsJson =
        http.get(http.url("/namespaces/%s/datasets/%s", namespaceName, datasetName));
    return Dataset.fromJson(bodyAsJson);
  }

  public List<Dataset> listDatasets(String namespaceName) {
    return listDatasets(namespaceName, DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public List<Dataset> listDatasets(@NonNull String namespaceName, int limit, int offset) {
    final String bodyAsJson =
        http.get(
            http.url("/namespaces/%s/datasets", newQueryParamsWith(limit, offset), namespaceName));
    return Datasets.fromJson(bodyAsJson).getValue();
  }

  public Dataset tagDatasetWith(
      @NonNull String namespaceName, @NonNull String datasetName, @NonNull String tagName) {
    final String bodyAsJson =
        http.post(
            http.url("/namespaces/%s/datasets/%s/tags/%s", namespaceName, datasetName, tagName));
    return Dataset.fromJson(bodyAsJson);
  }

  public Dataset tagFieldWith(
      @NonNull String namespaceName,
      @NonNull String datasetName,
      @NonNull String fieldName,
      @NonNull String tagName) {
    final String bodyAsJson =
        http.post(
            http.url(
                "/namespaces/%s/datasets/%s/fields/%s/tags/%s",
                namespaceName, datasetName, fieldName, tagName));
    return Dataset.fromJson(bodyAsJson);
  }

  public Job createJob(
      @NonNull String namespaceName, @NonNull String jobName, @NonNull JobMeta jobMeta) {
    final String bodyAsJson =
        http.put(http.url("/namespaces/%s/jobs/%s", namespaceName, jobName), jobMeta.toJson());
    return Job.fromJson(bodyAsJson);
  }

  public Job getJob(@NonNull String namespaceName, @NonNull String jobName) {
    final String bodyAsJson = http.get(http.url("/namespaces/%s/jobs/%s", namespaceName, jobName));
    return Job.fromJson(bodyAsJson);
  }

  public List<Job> listJobs(String namespaceName) {
    return listJobs(namespaceName, DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public List<Job> listJobs(@NonNull String namespaceName, int limit, int offset) {
    final String bodyAsJson =
        http.get(http.url("/namespaces/%s/jobs", newQueryParamsWith(limit, offset), namespaceName));
    return Jobs.fromJson(bodyAsJson).getValue();
  }

  public Run createRun(String namespaceName, String jobName, RunMeta runMeta) {
    return createRun(namespaceName, jobName, runMeta, false);
  }

  public Run createRunAndStart(String namespaceName, String jobName, RunMeta runMeta) {
    return createRun(namespaceName, jobName, runMeta, true);
  }

  private Run createRun(
      @NonNull String namespaceName,
      @NonNull String jobName,
      @NonNull RunMeta runMeta,
      boolean markRunAsRunning) {
    final String bodyAsJson =
        http.post(
            http.url("/namespaces/%s/jobs/%s/runs", namespaceName, jobName), runMeta.toJson());
    final Run run = Run.fromJson(bodyAsJson);
    return (markRunAsRunning) ? markRunAsRunning(run.getId()) : run;
  }

  public Run getRun(@NonNull String runId) {
    final String bodyAsJson = http.get(http.url("/jobs/runs/%s", runId));
    return Run.fromJson(bodyAsJson);
  }

  public List<Run> listRuns(String namespaceName, String jobName) {
    return listRuns(namespaceName, jobName, DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public List<Run> listRuns(
      @NonNull String namespaceName, @NonNull String jobName, int limit, int offset) {
    final String bodyAsJson =
        http.get(
            http.url(
                "/namespaces/%s/jobs/%s/runs",
                newQueryParamsWith(limit, offset), namespaceName, jobName));
    return Runs.fromJson(bodyAsJson).getValue();
  }

  public Run markRunAs(String runId, RunState runState) {
    return markRunAs(runId, runState, null);
  }

  public Run markRunAs(String runId, @NonNull RunState runState, @Nullable Instant at) {
    switch (runState) {
      case RUNNING:
        return markRunAsRunning(runId, at);
      case COMPLETED:
        return markRunAsCompleted(runId, at);
      case ABORTED:
        return markRunAsAborted(runId, at);
      case FAILED:
        return markRunAsFailed(runId, at);
      default:
        throw new IllegalArgumentException(
            String.format("Unexpected run state: %s", runState.name()));
    }
  }

  public Run markRunAsRunning(String runId) {
    return markRunAsRunning(runId, null);
  }

  public Run markRunAsRunning(String runId, @Nullable Instant at) {
    return markRunWith("/jobs/runs/%s/start", runId, at);
  }

  public Run markRunAsCompleted(String runId) {
    return markRunAsCompleted(runId, null);
  }

  public Run markRunAsCompleted(String runId, @Nullable Instant at) {
    return markRunWith("/jobs/runs/%s/complete", runId, at);
  }

  public Run markRunAsAborted(String runId) {
    return markRunAsAborted(runId, null);
  }

  public Run markRunAsAborted(String runId, @Nullable Instant at) {
    return markRunWith("/jobs/runs/%s/abort", runId, at);
  }

  public Run markRunAsFailed(String runId) {
    return markRunAsFailed(runId, null);
  }

  public Run markRunAsFailed(String runId, @Nullable Instant at) {
    return markRunWith("/jobs/runs/%s/fail", runId, at);
  }

  private Run markRunWith(String pathTemplate, @NonNull String runId, @Nullable Instant at) {
    final String bodyAsJson = http.post(http.url(pathTemplate, newQueryParamsWith(at), runId));
    return Run.fromJson(bodyAsJson);
  }

  public Set<Tag> listTags() {
    return listTags(DEFAULT_LIMIT, DEFAULT_OFFSET);
  }

  public Set<Tag> listTags(int limit, int offset) {
    final String bodyAsJson = http.get(http.url("/tags", newQueryParamsWith(limit, offset)));
    return Tags.fromJson(bodyAsJson).getValue();
  }

  private Map<String, Object> newQueryParamsWith(@Nullable Instant at) {
    return (at == null) ? ImmutableMap.of() : ImmutableMap.of("at", ISO_INSTANT.format(at));
  }

  private Map<String, Object> newQueryParamsWith(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return ImmutableMap.of("limit", limit, "offset", offset);
  }

  public static final class Builder {
    @VisibleForTesting URL baseUrl;

    private Builder() {
      this.baseUrl = DEFAULT_BASE_URL;
    }

    public Builder baseUrl(@NonNull String baseUrlString) {
      return baseUrl(Utils.toUrl(baseUrlString));
    }

    public Builder baseUrl(@NonNull URL baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    public MarquezClient build() {
      return new MarquezClient(MarquezHttp.create(baseUrl, MarquezClient.Version.get()));
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  @Value
  static class Version {
    private static final String CONFIG_PROPERTIES = "config.properties";

    private static final String VERSION_PROPERTY_NAME = "version";
    private static final String VERSION_UNKNOWN = "unknown";

    @Getter String value;

    private Version(@NonNull final String value) {
      this.value = value;
    }

    static Version get() {
      final Properties properties = new Properties();
      try (final InputStream stream =
          MarquezClient.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTIES)) {
        properties.load(stream);
        return new Version(properties.getProperty(VERSION_PROPERTY_NAME, VERSION_UNKNOWN));
      } catch (IOException e) {
        log.warn("Failed to load properties file: {}", CONFIG_PROPERTIES, e);
      }
      return NO_VERSION;
    }

    public static Version NO_VERSION = new Version(VERSION_UNKNOWN);
  }

  @Value
  static class Namespaces {
    @Getter List<Namespace> value;

    @JsonCreator
    Namespaces(@JsonProperty("namespaces") final List<Namespace> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Namespaces fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Namespaces>() {});
    }
  }

  @Value
  static class Sources {
    @Getter List<Source> value;

    @JsonCreator
    Sources(@JsonProperty("sources") final List<Source> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Sources fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Sources>() {});
    }
  }

  @Value
  static class Datasets {
    @Getter List<Dataset> value;

    @JsonCreator
    Datasets(@JsonProperty("datasets") final List<Dataset> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Datasets fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Datasets>() {});
    }
  }

  @Value
  static class Jobs {
    @Getter List<Job> value;

    @JsonCreator
    Jobs(@JsonProperty("jobs") final List<Job> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Jobs fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Jobs>() {});
    }
  }

  @Value
  static class Runs {
    @Getter List<Run> value;

    @JsonCreator
    Runs(@JsonProperty("runs") final List<Run> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Runs fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Runs>() {});
    }
  }

  @Value
  static class Tags {
    @Getter Set<Tag> value;

    @JsonCreator
    Tags(@JsonProperty("tags") final Set<Tag> value) {
      this.value = ImmutableSet.copyOf(value);
    }

    static Tags fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Tags>() {});
    }
  }
}
