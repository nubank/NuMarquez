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

import static marquez.client.models.ModelGenerator.newHttpEndpointMeta;
import static org.assertj.core.api.Assertions.assertThat;

import marquez.client.UnitTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTests.class)
public class HttpEndpointMetaTest {
  private static final DatasetMeta META = newHttpEndpointMeta();
  private static final String JSON = JsonGenerator.newJsonFor(META);

  @Test
  public void testToJson() {
    final String actual = META.toJson();
    assertThat(actual).isEqualTo(JSON);
  }
}
