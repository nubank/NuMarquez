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
import marquez.db.DatasetEdgesDao;

@Slf4j
public class LineageService extends DelegatingLineageDao {

  public record UpstreamRunLineage(List<UpstreamRun> runs) {
  }

  public record UpstreamRun(JobSummary job, RunSummary run, List<DatasetSummary> inputs) {
  }

  private final JobDao jobDao;
  private final RunDao runDao;
  private final DatasetEdgesDao datasetEdgesDao;

  public LineageService(LineageDao delegate, JobDao jobDao, RunDao runDao, DatasetEdgesDao datasetEdgesDao) {
    super(delegate);
    this.jobDao = jobDao;
    this.runDao = runDao;
    this.datasetEdgesDao = datasetEdgesDao;
  }

  /**
   * Retrieves regular lineage, from a node up to a specified depth.
   */
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

    if (jobData.isEmpty()) {
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
        .flatMap(
            jd -> Stream.concat(jd.getInputUuids().stream(), jd.getOutputUuids().stream()))
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

  /**
   * Given a dataset ID, return a minimal orphaned lineage graph with just that
   * dataset node.
   */
  private Lineage toLineageWithOrphanDataset(@NonNull DatasetId datasetId) {
    final DatasetData datasetData = getDatasetData(datasetId.getNamespace().getValue(), datasetId.getName().getValue());
    return new Lineage(
        ImmutableSortedSet.of(
            Node.dataset().data(datasetData).id(NodeId.of(datasetData.getId())).build()));
  }

  /**
   * Builds a lineage graph (Lineage) from sets of JobData and DatasetData.
   */
  private Lineage toLineage(Set<JobData> jobData, Set<DatasetData> datasets) {
    Set<Node> nodes = new LinkedHashSet<>();
    Map<UUID, DatasetData> datasetById = datasets.stream()
        .collect(Collectors.toMap(DatasetData::getUuid, Functions.identity()));
    Map<DatasetData, Set<UUID>> dsInputToJob = new HashMap<>();
    Map<DatasetData, Set<UUID>> dsOutputToJob = new HashMap<>();

    // build mapping from job data
    Map<UUID, JobData> jobDataMap = Maps.uniqueIndex(jobData, JobData::getUuid);

    // Build job nodes
    for (JobData data : jobData) {
      if (data == null) {
        log.error("Could not find job node for {}", jobData);
        continue;
      }

      Optional<JobData> parentJobData = getParentJobData(data.getParentJobUuid());
      parentJobData.ifPresent(
          parent -> {
            log.debug(
                "child: {}, parent: {} with UUID: {}",
                parent.getId().getName(),
                data.getParentJobName(),
                data);
          });

      Set<DatasetData> inputs = data.getInputUuids().stream()
          .map(datasetById::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      Set<DatasetData> outputs = data.getOutputUuids().stream()
          .map(datasetById::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());

      data.setInputs(buildDatasetId(inputs));
      data.setOutputs(buildDatasetId(outputs));

      inputs.forEach(ds -> dsInputToJob.computeIfAbsent(ds, e -> new HashSet<>()).add(data.getUuid()));
      outputs.forEach(ds -> dsOutputToJob.computeIfAbsent(ds, e -> new HashSet<>()).add(data.getUuid()));

      NodeId origin = NodeId.of(new JobId(data.getNamespace(), data.getName()));
      Node node = new Node(
          origin,
          NodeType.JOB,
          data, // job-level info
          buildDatasetEdge(inputs, origin), // inEdges
          buildDatasetEdge(origin, outputs)); // outEdges
      nodes.add(node);
    }

    // Build dataset nodes
    for (DatasetData dataset : datasets) {
      NodeId origin = NodeId.of(new DatasetId(dataset.getNamespace(), dataset.getName()));
      Node node = new Node(
          origin,
          NodeType.DATASET,
          dataset,
          buildJobEdge(dsOutputToJob.get(dataset), origin, jobDataMap),
          buildJobEdge(origin, dsInputToJob.get(dataset), jobDataMap));
      nodes.add(node);
    }

    return new Lineage(Lineage.withSortedNodes(Graph.directed().nodes(nodes).build()));
  }

  /**
   * Collects dataset IDs from a set of DatasetData.
   */
  private ImmutableSet<DatasetId> buildDatasetId(Set<DatasetData> datasetData) {
    if (datasetData == null) {
      return ImmutableSet.of();
    }
    return datasetData.stream()
        .map(ds -> new DatasetId(ds.getNamespace(), ds.getName()))
        .collect(ImmutableSet.toImmutableSet());
  }

  /**
   * Builds edges from a job node to zero or more other job nodes.
   */
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

  /**
   * Builds edges from a job node to zero or more datasets (or vice versa).
   */
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

  /**
   * Helper to build a NodeId from a DatasetData.
   */
  private NodeId buildEdge(DatasetData ds) {
    return NodeId.of(new DatasetId(ds.getNamespace(), ds.getName()));
  }

  /**
   * Helper to build a NodeId from a JobData.
   */
  private NodeId buildEdge(JobData e) {
    return NodeId.of(new JobId(e.getNamespace(), e.getName()));
  }

  /**
   * Try to get the UUID for a job node.
   */
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
   * Return the immediate upstream lineage for a given run, up to the specified
   * depth.
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

  // ---------------------
  // DIRECT LINEAGE METHOD
  // ---------------------
  /**
   * Retrieves direct lineage for a node up to a specified depth. It stops
   * expansion if it detects a
   * dataset with more than one outEdge (max-depth situation).
   */
  public Lineage directLineage(NodeId nodeId, int depth) {
    depth += 1;
    Optional<UUID> optionalUUID = getJobUuid(nodeId);
    if (optionalUUID.isEmpty()) {
      log.warn("Failed to get job associated with node '{}', returning orphan graph...", nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }
    UUID job = optionalUUID.get();

    Set<UUID> pending = new HashSet<>(Collections.singleton(job));
    Set<UUID> visited = new HashSet<>();
    Set<JobData> allJobData = new HashSet<>();
    Set<UUID> directDatasetIds = new HashSet<>();
    Map<UUID, Integer> jobDepthMap = new HashMap<>();
    jobDepthMap.put(job, 0);

    // Traverse until the level before the max depth
    for (int level = 0; level < depth && !pending.isEmpty(); level++) {
      Set<JobData> directLineage = getDirectLineage(pending);
      allJobData.addAll(directLineage);
      Set<UUID> nextJobs = new HashSet<>();
      visited.addAll(pending);
      pending.clear();

      for (JobData jd : directLineage) {
        // Collect the datasets for this job at the correct depth
        if (jobDepthMap.getOrDefault(jd.getUuid(), Integer.MAX_VALUE) == level) {
          directDatasetIds.addAll(jd.getInputUuids());
          directDatasetIds.addAll(jd.getOutputUuids());
        }

        // Only expand if there is still room before max depth.
        if (level < depth - 2) {
          Set<UUID> inputUuids = jd.getInputUuids();
          if (!inputUuids.isEmpty()) {
            Set<DatasetData> inputDatasets = getDatasetData(inputUuids);
            for (DatasetData ds : inputDatasets) {
              // If dataset has multiple outEdges, skip
              if (hasMultipleOutEdges(ds)) {
                log.debug("Max depth found for dataset '{}' due to multiple outEdges; branch stops.", ds.getId());
                continue;
              }
              Optional<UUID> maybeJob = getJobFromInputOrOutput(ds.getName().getValue(), ds.getNamespace().getValue());
              if (maybeJob.isPresent() && !visited.contains(maybeJob.get())) {
                UUID nextJob = maybeJob.get();
                nextJobs.add(nextJob);
                jobDepthMap.put(nextJob, level + 1);
              }
            }
          }
        }
      }
      // Only add nextJobs if not at the final expansion level.
      if (level < depth - 2) {
        pending.addAll(nextJobs);
      }
    }

    if (allJobData.isEmpty()) {
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }

    // Enrich the discovered jobs with their latest run
    for (JobData j : allJobData) {
      runDao.findRunByUuid(j.getCurrentRunUuid()).ifPresent(j::setLatestRun);
    }

    // Convert discovered dataset IDs into dataset objects
    Set<DatasetData> datasets = directDatasetIds.isEmpty()
        ? new HashSet<>()
        : new HashSet<>(getDatasetData(directDatasetIds));

    // If starting at a dataset node, ensure it’s included
    if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      DatasetData datasetData = getDatasetData(datasetId.getNamespace().getValue(), datasetId.getName().getValue());
      if (!directDatasetIds.contains(datasetData.getUuid())) {
        log.warn(
            "Found jobs {} which no longer share lineage with dataset '{}' - discarding",
            allJobData.stream().map(JobData::getId).toList(),
            nodeId.getValue());
        return toLineageWithOrphanDataset(datasetId);
      }
    }

    return toLineage(allJobData, datasets);
  }

  /**
   * Helper method to detect a multi outEdge dataset.
   * Currently requires an outEdges field or a DAO resource that can count them.
   */
  private boolean hasMultipleOutEdges(DatasetData ds) {
    // Retrieve outEdges via your DAO
    ImmutableSet<Edge> outEdges = datasetEdgesDao.outEdgesFor(ds.getUuid());
    return outEdges.size() > 1;
  }

  /**
   * Helper overrides from DelegatingLineageDao
   */
  @Override
  public Set<DatasetData> getDatasetData(Set<UUID> dsUuids) {
    if (dsUuids == null || dsUuids.isEmpty()) {
      return Collections.emptySet();
    }
    return super.getDatasetData(dsUuids);
  }

  @Override
  public DatasetData getDatasetData(String namespaceName, String datasetName) {
    return super.getDatasetData(namespaceName, datasetName);
  }

  @Override
  public Optional<UUID> getJobFromInputOrOutput(String datasetName, String namespaceName) {
    return super.getJobFromInputOrOutput(datasetName, namespaceName);
  }
}