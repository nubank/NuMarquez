UPDATE jobs SET symlink_target_uuid=q.target_uuid
FROM (
         SELECT j.uuid, j.namespace_name, j.name, j.simple_name, jv.uuid AS target_uuid, jv.simple_name
         FROM jobs_view j INNER JOIN jobs_view jv ON j.namespace_name=jv.namespace_name AND j.name=jv.name AND j.simple_name != jv.simple_name
         WHERE j.symlink_target_uuid IS NULL
           AND jv.symlink_target_uuid IS NULL
           AND j.parent_job_uuid IS NULL) q
WHERE jobs.uuid=q.uuid;

ALTER TABLE jobs RENAME COLUMN name TO simple_name;
ALTER TABLE jobs ADD COLUMN name varchar;
ALTER TABLE jobs ADD COLUMN aliases varchar[];

WITH RECURSIVE
    job_fqn AS (SELECT j.uuid,
                   j.simple_name AS simple_name,
                   j.simple_name AS name
            FROM jobs j
            WHERE j.parent_job_uuid IS NULL
            UNION
            SELECT j1.uuid,
                   j1.simple_name AS simple_name,
                   f.name || '.' || j1.simple_name AS name
            FROM jobs j1
            INNER JOIN job_fqn f ON f.uuid = j1.parent_job_uuid)
UPDATE jobs SET simple_name=f.simple_name, name=f.name
FROM job_fqn f
WHERE jobs.uuid=f.uuid;

ALTER TABLE jobs ALTER COLUMN name SET NOT NULL;

ALTER TABLE jobs DROP CONSTRAINT unique_jobs_namespace_uuid_name_parent;
ALTER TABLE jobs ADD CONSTRAINT unique_jobs_namespace_uuid_name_parent UNIQUE (namespace_uuid, name);