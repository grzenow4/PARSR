package allezon.service;

import static allezon.constant.Constants.MAX_LIST_SIZE;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.ListOperation;

import allezon.domain.UserTagEvent;

@Service
public class AerospikeService {
    private final AerospikeClient client;

    @Autowired
    public AerospikeService(AerospikeClient client) {
        this.client = client;
    }

    public Record get(Policy policy, Key key) {
        return client.get(policy, key);
    }



    private WritePolicy createWritePolicy(Record record) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;  
        writePolicy.generationPolicy = (record != null) ? GenerationPolicy.EXPECT_GEN_EQUAL : GenerationPolicy.NONE;
        writePolicy.generation = (record != null) ? record.generation : 0;
        return writePolicy;
    }

    private Policy createReadPolicy() {
        Policy readPolicy = new Policy(client.readPolicyDefault);
        readPolicy.socketTimeout = 1000;
        readPolicy.totalTimeout = 1000;
        return readPolicy;
    }

    private void removeOutdatedRecords(Record newRecord, int currentSize, Key key) {
        Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
        WritePolicy writePolicy = createWritePolicy(newRecord);
        client.operate(writePolicy, key, trimOperation);
    }
}
