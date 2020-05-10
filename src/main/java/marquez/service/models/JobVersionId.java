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

package marquez.service.models;

import lombok.NonNull;
import lombok.Value;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;

/** The unique identifier of a Job Version in the lineage graph */
@Value
public class JobVersionId {
  @NonNull NamespaceName namespace;
  @NonNull JobName jobName;
  @NonNull Version version;
}
