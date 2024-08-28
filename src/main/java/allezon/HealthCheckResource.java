package allezon;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckResource {
    // Health check endpoint for load balancer.
    @GetMapping("/health")
    public ResponseEntity<Void> heartbeat() {
        return ResponseEntity.noContent().build();
    }
}
