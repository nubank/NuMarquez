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

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** An exception thrown to indicate an HTTP error. */
@NoArgsConstructor
@ToString
public final class MarquezHttpException extends MarquezClientException {
  private static final long serialVersionUID = 1L;

  @Getter @Nullable private Integer status;
  @Getter @Nullable private String message;

  /** Constructs a {@code MarquezHttpException} with the HTTP error {@code error}. */
  MarquezHttpException(final MarquezHttp.HttpError error) {
    this.status = error.getStatus();
    this.message = error.getMessage();
  }
}
