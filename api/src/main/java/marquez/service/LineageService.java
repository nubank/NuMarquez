/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetId;
import marquez.common.models.JobId;
import marquez.common.models.RunId;
import marquez.db.JobDao;
import marquez.db.LineageDao;
import marquez.db.RunDao;
import marquez.db.models.JobRow;
import marquez.service.DelegatingDaos.DelegatingLineageDao;
import marquez.service.models.DatasetData;
import marquez.service.models.Edge;
import marquez.service.models.Graph;
import marquez.service.models.JobData;
import marquez.service.models.Lineage;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import marquez.service.models.NodeType;
import marquez.service.models.Run;

@Slf4j
public class LineageService extends DelegatingLineageDao {

  public record UpstreamRunLineage(List<UpstreamRun> runs) {
  }

  public record UpstreamRun(JobSummary job, RunSummary run, List<DatasetSummary> inputs) {
  }

  private final JobDao jobDao;

  private final RunDao runDao;

  public LineageService(LineageDao delegate, JobDao jobDao, RunDao runDao) {
    super(delegate);
    this.jobDao = jobDao;
    this.runDao = runDao;
  }

  // TODO make input parameters easily extendable if adding more options like
  // 'withJobFacets'
  public Lineage lineage(NodeId nodeId, int depth) {
    log.debug("Attempting to get lineage for node '{}' with depth '{}'", nodeId.getValue(), depth);
    Optional<UUID> optionalUUID = getJobUuid(nodeId);
    if (optionalUUID.isEmpty()) {
      log.warn(
          "Failed to get job associated with node '{}', returning orphan graph...",
          nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }
    UUID job = optionalUUID.get();
    log.debug("Attempting to get lineage for job '{}'", job);
    Set<JobData> jobData = getLineage(Collections.singleton(job), depth);

    // Ensure job data is not empty, an empty set cannot be passed to
    // LineageDao.getCurrentRuns() or
    // LineageDao.getCurrentRunsWithFacets().
    if (jobData.isEmpty()) {
      // Log warning, then return an orphan lineage graph; a graph should contain at
      // most one
      // job->dataset relationship.
      log.warn(
          "Failed to get lineage for job '{}' associated with node '{}', returning orphan graph...",
          job,
          nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }

    for (JobData j : jobData) {
      Optional<Run> run = runDao.findRunByUuid(j.getCurrentRunUuid());
      run.ifPresent(j::setLatestRun);
    }

    Set<UUID> datasetIds = jobData.stream()
        .flatMap(jd -> Stream.concat(jd.getInputUuids().stream(), jd.getOutputUuids().stream()))
        .collect(Collectors.toSet());
    Set<DatasetData> datasets = new HashSet<>();
    if (!datasetIds.isEmpty()) {
      datasets.addAll(this.getDatasetData(datasetIds));
    }

    if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      DatasetData datasetData = this.getDatasetData(datasetId.getNamespace().getValue(),
          datasetId.getName().getValue());

      if (!datasetIds.contains(datasetData.getUuid())) {
        log.warn(
            "Found jobs {} which no longer share lineage with dataset '{}' - discarding",
            jobData.stream().map(JobData::getId).toList(),
            nodeId.getValue());
        return toLineageWithOrphanDataset(nodeId.asDatasetId());
      }
    }
    return toLineage(jobData, datasets);
  }

  private Lineage toLineageWithOrphanDataset(@NonNull DatasetId datasetId) {
    final DatasetData datasetData = getDatasetData(datasetId.getNamespace().getValue(), datasetId.getName().getValue());
    return new Lineage(
        ImmutableSortedSet.of(
            Node.dataset().data(datasetData).id(NodeId.of(datasetData.getId())).build()));
  }

  private Lineage toLineage(Set<JobData> jobData, Set<DatasetData> datasets) {
    Set<Node> nodes = new LinkedHashSet<>();

    // Build mapping for later
    Map<UUID, DatasetData> datasetById = datasets.stream()
        .collect(Collectors.toMap(DatasetData::getUuid, Functions.identity()));

    Map<DatasetData, Set<UUID>> dsInputToJob = new HashMap<>();
    Map<DatasetData, Set<UUID>> dsOutputToJob = new HashMap<>();

    // Build jobs map (deduplicated)
    Map<UUID, JobData> jobDataMap = jobData.stream()
        .collect(Collectors.toMap(
            JobData::getUuid,
            job -> job,
            (existing, replacement) -> existing));

    // Process each JobData (using 'data' as the declared variable)
    for (JobData data : jobData) {
      Optional<JobData> parentJobData = getParentJobData(data.getParentJobUuid());
      parentJobData.ifPresent(parent -> { 
        log.debug(
            "child: {}, parent: {} with UUID: {}",
            parent.getId().getName(),
            data.getParentJobName(),
            data);
      });

      // Map job input and output UUIDs to their corresponding DatasetData objects
      Set<DatasetData> inputs = data.getInputUuids().stream()
          .map(datasetById::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      Set<DatasetData> outputs = data.getOutputUuids().stream()
          .map(datasetById::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());

      // Update job data with resolved dataset IDs
      data.setInputs(buildDatasetId(inputs));
      data.setOutputs(buildDatasetId(outputs));

      // Build reverse mapping from DatasetData to JobData UUIDs
      inputs.forEach(ds -> dsInputToJob.computeIfAbsent(ds, e -> new HashSet<>()).add(data.getUuid()));
      outputs.forEach(ds -> dsOutputToJob.computeIfAbsent(ds, e -> new HashSet<>()).add(data.getUuid()));

      // Create a Node for the job
      NodeId origin = NodeId.of(new JobId(data.getNamespace(), data.getName()));
      Node jobNode = new Node(
          origin,
          NodeType.JOB,
          data,
          buildDatasetEdge(inputs, origin),
          buildDatasetEdge(origin, outputs));
      nodes.add(jobNode);
    }

    // Process each DatasetData to create dataset nodes that connect to jobs
    for (DatasetData dataset : datasets) {
      NodeId origin = NodeId.of(new DatasetId(dataset.getNamespace(), dataset.getName()));
      Node datasetNode = new Node(
          origin,
          NodeType.DATASET,
          dataset,
          buildJobEdge(dsOutputToJob.get(dataset), origin, jobDataMap),
          buildJobEdge(origin, dsInputToJob.get(dataset), jobDataMap));
      nodes.add(datasetNode);
    }

    return new Lineage(Lineage.withSortedNodes(Graph.directed().nodes(nodes).build()));
  }

  private ImmutableSet<DatasetId> buildDatasetId(Set<DatasetData> datasetData) {
    if (datasetData == null) {
      return ImmutableSet.of();
    }
    return datasetData.stream()
        .map(ds -> new DatasetId(ds.getNamespace(), ds.getName()))
        .collect(ImmutableSet.toImmutableSet());
  }

  private ImmutableSet<Edge> buildJobEdge(
      NodeId origin, Set<UUID> uuids, Map<UUID, JobData> jobDataMap) {
    if (uuids == null) {
      return ImmutableSet.of();
    }
    return uuids.stream()
        .map(jobDataMap::get)
        .filter(Objects::nonNull)
        .map(j -> new Edge(origin, buildEdge(j)))
        .collect(ImmutableSet.toImmutableSet());
  }

  private ImmutableSet<Edge> buildJobEdge(
      Set<UUID> uuids, NodeId origin, Map<UUID, JobData> jobDataMap) {
    if (uuids == null) {
      return ImmutableSet.of();
    }
    return uuids.stream()
        .map(jobDataMap::get)
        .filter(Objects::nonNull)
        .map(j -> new Edge(buildEdge(j), origin))
        .collect(ImmutableSet.toImmutableSet());
  }

  private ImmutableSet<Edge> buildDatasetEdge(NodeId nodeId, Set<DatasetData> datasetData) {
    if (datasetData == null) {
      return ImmutableSet.of();
    }
    return datasetData.stream()
        .map(ds -> new Edge(nodeId, buildEdge(ds)))
        .collect(ImmutableSet.toImmutableSet());
  }

  private ImmutableSet<Edge> buildDatasetEdge(Set<DatasetData> datasetData, NodeId nodeId) {
    if (datasetData == null) {
      return ImmutableSet.of();
    }
    return datasetData.stream()
        .map(ds -> new Edge(buildEdge(ds), nodeId))
        .collect(ImmutableSet.toImmutableSet());
  }

  private NodeId buildEdge(DatasetData ds) {
    return NodeId.of(new DatasetId(ds.getNamespace(), ds.getName()));
  }

  private NodeId buildEdge(JobData e) {
    return NodeId.of(new JobId(e.getNamespace(), e.getName()));
  }

  public Optional<UUID> getJobUuid(NodeId nodeId) {
    if (nodeId.isJobType()) {
      JobId jobId = nodeId.asJobId();
      return jobDao
          .findJobByNameAsRow(jobId.getNamespace().getValue(), jobId.getName().getValue())
          .map(JobRow::getUuid);
    } else if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      return getJobFromInputOrOutput(
          datasetId.getName().getValue(), datasetId.getNamespace().getValue());
    } else {
      throw new NodeIdNotFoundException(
          String.format("Node '%s' must be of type dataset or job!", nodeId.getValue()));
    }
  }

  /**
   * Returns the upstream lineage for a given run. Recursively: run -> dataset
   * version it read from
   * -> the run that produced it
   *
   * @param runId the run to get upstream lineage from
   * @param depth the maximum depth of the upstream lineage
   * @return the upstream lineage for that run up to `detph` levels
   */
  public UpstreamRunLineage upstream(@NotNull RunId runId, int depth) {
    List<UpstreamRunRow> upstreamRuns = getUpstreamRuns(runId.getValue(), depth);
    Map<RunId, List<UpstreamRunRow>> collect = upstreamRuns.stream()
        .collect(groupingBy(r -> r.run().id(), LinkedHashMap::new, toList()));
    List<UpstreamRun> runs = collect.entrySet().stream()
        .map(
            row -> {
              UpstreamRunRow upstreamRunRow = row.getValue().get(0);
              List<DatasetSummary> inputs = row.getValue().stream()
                  .map(UpstreamRunRow::input)
                  .filter(i -> i != null)
                  .collect(toList());
              return new UpstreamRun(upstreamRunRow.job(), upstreamRunRow.run(), inputs);
            })
        .collect(toList());
    return new UpstreamRunLineage(runs);
  }

  /**
   * Método helper para buscar jobs que são produtores DIRETOS dos datasets de
   * entrada.
   * Realiza a iteração em níveis até o máximo de profundidade especificado.
   *
   * @param initialJobIds conjunto com os ID(s) de job inicial(is)
   * @param maxDepth      profundidade máxima para busca
   * @return conjunto com todos os JobData encontrados
   */
  private Set<JobData> fetchDirectJobs(Set<UUID> initialJobIds, int maxDepth, Boolean isUpstream) {
    Map<UUID, JobData> allJobsMap = new HashMap<>();
    Set<UUID> processedJobs = new HashSet<>();
    Set<UUID> currentLevelJobs = new HashSet<>(initialJobIds);

    // Add initial jobs to the map first (get job details for input jobs)
    Set<JobData> initialJobs = getDirectLineage(currentLevelJobs);
    for (JobData jobData : initialJobs) {
      // Only add jobs that are in our initial set
      if (initialJobIds.contains(jobData.getUuid())) {
        allJobsMap.put(jobData.getUuid(), jobData);
      }
    }

    // Now process the jobs
    for (int depth = 0; depth < maxDepth; depth++) {
      log.debug("Processing depth {} for lineage", depth);

      // If there are no jobs to process at this level, we're done
      if (currentLevelJobs.isEmpty()) {
        break;
      }

      // Mark current jobs as processed
      processedJobs.addAll(currentLevelJobs);

      // Busca os jobs produtores ou consumidores diretos dependendo da direção
      Set<JobData> directRelatedJobs = new HashSet<>();
      if(isUpstream) {
        directRelatedJobs = getDirectUpstreamJobs(currentLevelJobs);
      } else {
        directRelatedJobs = getDirectDownstreamJobs(currentLevelJobs);
      }

      Set<UUID> nextLevelJobs = new HashSet<>();
      for (JobData jobData : directRelatedJobs) {
        // Skip if we've already processed this job
        if (!allJobsMap.containsKey(jobData.getUuid())) {
          allJobsMap.put(jobData.getUuid(), jobData);
          if (!processedJobs.contains(jobData.getUuid())) {
            nextLevelJobs.add(jobData.getUuid());
          }
        }
      }

      // Prepara para o próximo nível
      currentLevelJobs = nextLevelJobs;
    }

    return new HashSet<>(allJobsMap.values());
  }

  /**
   * Retrieves direct lineage information for a given node up to the specified
   * depth. If the node
   * corresponds to a dataset and no associated lineage is found, an orphan
   * dataset graph is returned.
   *
   * @param nodeId the node (dataset or job) to retrieve the lineage for
   * @param depth  the maximum depth of lineage traversal
   * @return a {@link Lineage} object containing the collected lineage
   */
  public Lineage directLineage(NodeId nodeId, int maxDepth) {
    Optional<UUID> optionalUUID = getJobUuid(nodeId);
    if (optionalUUID.isEmpty()) {
      log.warn(
          "Failed to get job associated with node '{}', returning orphan graph...",
          nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }
    UUID centralJobUuid = optionalUUID.get();

    // Recupera os jobs upstream e downstream usando os métodos helper
    Set<JobData> upstreamJobs = fetchDirectJobs(Collections.singleton(centralJobUuid), maxDepth, true);
    Set<JobData> downstreamJobs = fetchDirectJobs(Collections.singleton(centralJobUuid), maxDepth, false);

    // Combina os resultados eliminando duplicatas
    Set<JobData> allJobsData = new HashSet<>();
    allJobsData.addAll(upstreamJobs);
    allJobsData.addAll(downstreamJobs);

    if (allJobsData.isEmpty()) {
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }

    // Adiciona informações de execução a cada job
    for (JobData jobData : allJobsData) {
      runDao.findRunByUuid(jobData.getCurrentRunUuid()).ifPresent(jobData::setLatestRun);
    }

    Set<UUID> allDatasetIds = allJobsData.stream()
        .flatMap(jd -> Stream.concat(jd.getInputUuids().stream(), jd.getOutputUuids().stream()))
        .collect(Collectors.toSet());

    Set<DatasetData> allDatasets = allDatasetIds.isEmpty()
        ? new HashSet<>()
        : new HashSet<>(getDatasetData(allDatasetIds));

    if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      DatasetData datasetData = getDatasetData(datasetId.getNamespace().getValue(), datasetId.getName().getValue());
      if (!allDatasetIds.contains(datasetData.getUuid())) {
        log.warn(
            "Found jobs {} which no longer share lineage with dataset '{}' - discarding",
            allJobsData.stream().map(JobData::getId).toList(),
            nodeId.getValue());
        return toLineageWithOrphanDataset(datasetId);
      }
    }

    return toLineage(allJobsData, allDatasets);
  }

  /**
   * Fetch dataset information for the given set of dataset UUIDs.
   */
  @Override
  public Set<DatasetData> getDatasetData(Set<UUID> dsUuids) {
    if (dsUuids == null || dsUuids.isEmpty()) {
      return Collections.emptySet();
    }
    return super.getDatasetData(dsUuids);
  }

  /**
   * Fetch dataset information for the given namespace and dataset name.
   */
  @Override
  public DatasetData getDatasetData(String namespaceName, String datasetName) {
    return super.getDatasetData(namespaceName, datasetName);
  }

  /**
   * Looks up the latest Job UUID that produced or consumed the dataset by name
   * and namespace.
   */
  @Override
  public Optional<UUID> getJobFromInputOrOutput(String datasetName, String namespaceName) {
    return super.getJobFromInputOrOutput(datasetName, namespaceName);
  }
}
