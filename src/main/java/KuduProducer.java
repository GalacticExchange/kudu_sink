import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.flume.sink.KuduOperationsProducer;

import java.util.Collections;
import java.util.List;


public class KuduProducer implements KuduOperationsProducer {

    private KuduTable table;

    public KuduProducer() {
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void initialize(KuduTable kuduTable) {
        this.table = kuduTable;
    }

    @Override
    public List<Operation> getOperations(org.apache.flume.Event event) {
        try {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addString("message", new String(event.getBody()));
            return Collections.singletonList(insert);
        } catch (Exception e) {
            throw new FlumeException("Failed to create Kudu Insert object", e);
        }
    }

    @Override
    public void close() {

    }

}
