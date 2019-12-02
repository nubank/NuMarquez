CREATE TABLE tags (
  uuid        UUID PRIMARY KEY,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  name        VARCHAR(255) NOT NULL UNIQUE,
  description TEXT
);
CREATE TABLE dataset_tag_mapping (
  dataset_uuid UUID REFERENCES datasets(uuid),
  tag_uuid UUID REFERENCES tags(uuid),
  tagged_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (tag_uuid, dataset_uuid)
);
CREATE TABLE dataset_fields_tag_mapping (
  dataset_field_uuid UUID REFERENCES dataset_fields(uuid),
  tag_uuid UUID REFERENCES tags(uuid),
  tagged_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (tag_uuid, dataset_field_uuid)
);