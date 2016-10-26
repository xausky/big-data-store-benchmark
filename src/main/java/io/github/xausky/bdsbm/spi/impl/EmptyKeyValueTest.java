package io.github.xausky.bdsbm.spi.impl;

import io.github.xausky.bdsbm.spi.KeyValueTest;

import java.util.List;
import java.util.Map;

/**
 * Created by xausky on 9/29/16.
 */
public class EmptyKeyValueTest implements KeyValueTest {
    private List<String> data;
    public void init() {

    }

    public void store(List<String> data) {
        this.data = data;
    }

    public int scan() throws Exception {
        return data.size();
    }

    public void destroy() {

    }
}
