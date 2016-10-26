package io.github.xausky.bdsbm.spi.impl;

import io.github.xausky.bdsbm.spi.KeyValueTest;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xausky on 9/30/16.
 */
public class KuduKeyValueTest implements KeyValueTest {
    private static String KUDU_MASTER = System.getProperty("KUDU_MASTER", "kudu_master_1");
    private static String KUDU_TABLE_NAME = System.getProperty("KUDU_TABLE_NAME", "test")+System.currentTimeMillis();
    private KuduTable table;
    private KuduClient client;
    private KuduSession session;

    public void init() throws KuduException {
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        List<ColumnSchema> columns = new ArrayList(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("hash", Type.INT32).build());
        Schema schema = new Schema(columns);
        List<String> rangeColumns = new ArrayList<String>(1);
        rangeColumns.add("id");
        client.createTable(KUDU_TABLE_NAME, schema, new CreateTableOptions().setRangePartitionColumns(rangeColumns));
        table = client.openTable(KUDU_TABLE_NAME);
        session = client.newSession();
    }

    public void store(List<String> data) throws KuduException {
        int count = 0;
        for (String value : data) {
            count++;
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt(0,count);
            row.addString(1,value);
            row.addInt(2,value.hashCode());
            session.apply(insert);
        }
    }

    public int scan() throws Exception {
        List<String> projectColumns = new ArrayList<String>(1);
        projectColumns.add("value");
        projectColumns.add("hash");
        KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
        int count=0;
        while (scanner.hasMoreRows()){
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()){
                count++;
                RowResult result = results.next();
                String value = result.getString("value");
                int hash = result.getInt("hash");
                if(value.hashCode()!=hash){
                    throw new Exception("hash error");
                }
            }
        }
        return count;
    }

    public void destroy() throws KuduException {
        session.close();
        client.close();
    }
}
