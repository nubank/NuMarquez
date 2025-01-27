package marquez.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import javax.ws.rs.core.Response;
import marquez.service.ServiceFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

@ExtendWith(DropwizardExtensionsSupport.class)
public class NuHealthCheckResourceTest extends BaseResourceIntegrationTest {

  @Mock private ServiceFactory serviceFactory;

  private NuHealthCheckResource nuHealthCheckResource;

  @BeforeEach
  public void setUp() {
    nuHealthCheckResource = new NuHealthCheckResource(serviceFactory);
  }

  @Test
  void testGet() {
    Response response = nuHealthCheckResource.get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
  }
}
