import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.flume.sink.KuduOperationsProducer;
import org.json.JSONObject;

import java.util.Collections;
import java.util.List;


public class KuduJsonProducer implements KuduOperationsProducer {

    private KuduTable table;

    public KuduJsonProducer() {
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
            JSONObject obj = new JSONObject(new String(event.getBody()));
            System.out.println("!!!!!!!!!!!!!!" + obj.toString());
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            /* boolean
            8-bit signed integer
            16-bit signed integer
            32-bit signed integer
            64-bit signed integer
            unixtime_micros (64-bit microseconds since the Unix epoch)
            single-precision (32-bit) IEEE-754 floating-point number
            double-precision (64-bit) IEEE-754 floating-point number
            UTF-8 encoded string (up to 64KB)
            binary (up to 64KB)*/
            try {
                for (String key : obj.keySet()) {
                    System.out.println("!!!!!!!!!!!!!!" + key + " " + obj.get(key).toString());
                    Object o = obj.get(key);
                    if (o instanceof String) {
                        row.addString(key, obj.getString(key));
                    } else if (o instanceof Integer) {
                        row.addInt(key, obj.getInt(key));
                    } else if (o instanceof Boolean) {
                        row.addBoolean(key, obj.getBoolean(key));
                    } else if (o instanceof Long) {
                        row.addLong(key, obj.getLong(key));
                    } else if (o instanceof Double) {
                        row.addDouble(key, obj.getDouble(key));
                    } else if (o instanceof byte[]) {
                        row.addBinary(key, obj.getString(key).getBytes());
                    } else if (o instanceof Float) {
                        // ??
                        row.addFloat(key, (float) obj.getDouble(key));
                    } else if (o instanceof Short) {
                        // ??
                        row.addShort(key, (short) obj.getInt(key));
                    } else {
                        row.addString(key, obj.getString(key));
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
                throw e;
            }
            return Collections.singletonList(insert);
        } catch (Exception e) {
            throw new FlumeException("Failed to create Kudu Insert object", e);
        }
    }

    @Override
    public void close() {

    }

}
