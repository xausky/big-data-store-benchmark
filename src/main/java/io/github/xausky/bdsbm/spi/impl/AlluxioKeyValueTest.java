package io.github.xausky.bdsbm.spi.impl;

import alluxio.AlluxioURI;
import alluxio.client.keyvalue.*;
import io.github.xausky.bdsbm.spi.KeyValueTest;

import java.util.List;

/**
 * Created by xausky on 10/1/16.
 */
public class AlluxioKeyValueTest implements KeyValueTest {
    private KeyValueSystem kvs;
    public void init() throws Exception {
        kvs = KeyValueSystem.Factory.create();
    }

    public void store(List<String> data) throws Exception {
        KeyValueStoreWriter writer = kvs.createStore(new AlluxioURI("alluxio://test/"));
        for(String value:data){
            writer.put(value.getBytes(),intToByteArray(value.hashCode()));
        }
        writer.close();
    }

    public int scan() throws Exception {
        KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://test/"));
        KeyValueIterator iterator = reader.iterator();
        int count = 0;
        while(iterator.hasNext()){
            KeyValuePair pair = iterator.next();
            String value = pair.getKey().toString();
            int hash = byteArrayToInt(pair.getValue().array(),0);
            if(value.hashCode()!=hash){
                throw new Exception("hash error");
            }
            count++;
        }
        reader.close();
        return count;
    }

    public void destroy() throws Exception {

    }

    private byte[] intToByteArray (final int integer) {
        int byteNum = (40 - Integer.numberOfLeadingZeros (integer < 0 ? ~integer : integer)) / 8;
        byte[] byteArray = new byte[4];

        for (int n = 0; n < byteNum; n++)
            byteArray[3 - n] = (byte) (integer >>> (n * 8));

        return (byteArray);
    }

    public static int byteArrayToInt(byte[] b, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i + offset] & 0x000000FF) << shift;
        }
        return value;
    }
}
