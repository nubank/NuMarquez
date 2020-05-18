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

package marquez.client.models;

import com.fasterxml.jackson.core.type.TypeReference;
import java.net.URL;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import marquez.client.Utils;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class Job extends JobMeta {
  @Getter @NonNull private final String name;
  @Getter @NonNull private final Instant createdAt;
  @Getter @NonNull private final Instant updatedAt;

  public Job(
      final JobType type,
      @NonNull final String name,
      @NonNull final Instant createdAt,
      @NonNull final Instant updatedAt,
      final Set<String> inputs,
      final Set<String> outputs,
      final URL location,
      final String description,
      final Map<String, String> context) {
    super(type, inputs, outputs, location, description, context);
    this.name = name;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
  }

  public static Job fromJson(@NonNull final String json) {
    return Utils.fromJson(json, new TypeReference<Job>() {});
  }
}
