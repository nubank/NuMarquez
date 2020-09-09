package marquez.client;

import javax.annotation.Nullable;

/**
 * The backend contract for sending Marquez instrumentation. Information operations can be sent
 * synchronously or asynchronously over various protocols
 */
public interface Backend {

  void put(String path, String json);

  default void post(String path) {
    post(path, null);
  }

  void post(String path, @Nullable String json);
}
