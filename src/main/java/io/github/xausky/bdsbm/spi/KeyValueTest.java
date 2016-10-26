package io.github.xausky.bdsbm.spi;

import org.apache.kudu.client.KuduException;

import java.util.List;
import java.util.Map;

/**
 * Created by xausky on 9/29/16.
 */
public interface KeyValueTest {
    public void init() throws Exception;
    public void store(List<String> data) throws Exception;
    public int scan() throws Exception;
    public void destroy() throws Exception;
}
