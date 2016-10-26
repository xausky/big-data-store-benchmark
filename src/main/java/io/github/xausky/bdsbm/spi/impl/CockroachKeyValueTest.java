package io.github.xausky.bdsbm.spi.impl;

import io.github.xausky.bdsbm.spi.KeyValueTest;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.PartialRow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * Created by xausky on 9/30/16.
 */
public class CockroachKeyValueTest implements KeyValueTest {
    private String tableName = "test"+System.currentTimeMillis();
    private Connection db;
    public void init() throws Exception {
        Class.forName("org.postgresql.Driver");
        db = DriverManager.getConnection("jdbc:postgresql://cockroach_master_1:26257/test", "root", "");
        db.createStatement().execute("CREATE TABLE "+tableName+" (id INT PRIMARY KEY,value TEXT,hash INTEGER)");
    }

    public void store(List<String> data) throws Exception {
        PreparedStatement preparedStatement = db.prepareStatement("INSERT INTO "
                + tableName + " (id, value, hash) VALUES (?, ?, ?)");
        int count = 0;
        for (String value:data) {
            count++;
            preparedStatement.setInt(1,count);
            preparedStatement.setString(2,value);
            preparedStatement.setInt(3,value.hashCode());
            preparedStatement.execute();
        }
    }

    public int scan() throws Exception {
        int count = 0;
        ResultSet results = db.createStatement().executeQuery("SELECT * FROM "+tableName);
        while (results.next()){
            count++;
            String value = results.getString(2);
            int hash = results.getInt(3);
            if(value.hashCode()!=hash){
                throw new Exception("hash error");
            }
        }
        return 0;
    }

    public void destroy() throws Exception {
        db.close();
    }
}
