package allezon.config;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.Replica;

@Configuration
public class AerospikeConfig {
    @Value("${aerospike.seeds}")
    private String[] aerospikeSeeds;

    @Value("${aerospike.port}")
    private int aerospikePort;

    @Bean
    public ClientPolicy defaultClientPolicy() {
        ClientPolicy defaultClientPolicy = new ClientPolicy();
        defaultClientPolicy.readPolicyDefault.replica = Replica.MASTER_PROLES;
        defaultClientPolicy.readPolicyDefault.socketTimeout = 1000;
        defaultClientPolicy.readPolicyDefault.totalTimeout = 1000;
        defaultClientPolicy.writePolicyDefault.socketTimeout = 15000;
        defaultClientPolicy.writePolicyDefault.totalTimeout = 15000;
        defaultClientPolicy.writePolicyDefault.maxRetries = 1;
        defaultClientPolicy.writePolicyDefault.commitLevel = CommitLevel.COMMIT_MASTER;
        defaultClientPolicy.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;
        return defaultClientPolicy;
    }

        @Bean
    public AerospikeClient aerospikeClient(ClientPolicy clientPolicy) {
        Host[] hosts = Arrays.stream(aerospikeSeeds)
                             .map(seed -> new Host(seed, aerospikePort))
                             .toArray(Host[]::new);
        return new AerospikeClient(clientPolicy, hosts);
    }
}
