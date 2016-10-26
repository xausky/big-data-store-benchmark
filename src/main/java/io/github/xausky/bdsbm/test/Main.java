package io.github.xausky.bdsbm.test;

import io.github.xausky.bdsbm.spi.KeyValueTest;
import io.github.xausky.bdsbm.spi.impl.*;

import java.util.*;
import java.util.jar.Pack200;
import java.util.logging.Logger;

/**
 * Created by xausky on 9/29/16.
 */
public class Main {
    public static Logger logger = Logger.getLogger(Main.class.getName());
    public static void main(String[] args){
        try {
            int testIndex = 0;
            try {
                testIndex = Integer.parseInt(System.getProperty("testIndex"));
            }catch (Exception e){
                testIndex = 5;
            }
            int dataSize = 0;
            try {
                dataSize = Integer.parseInt(System.getProperty("dataSize"));
            }catch (Exception e){
                dataSize = 10000;
            }
            int keyLength = 0;
            try {
                keyLength = Integer.parseInt(System.getProperty("keyLength"));
            }catch (Exception e){
                keyLength = 16;
            }
            int valueLength = 0;
            try {
                valueLength = Integer.parseInt(System.getProperty("valueLength"));
            }catch (Exception e){
                valueLength = 32;
            }
            KeyValueTest test = null;
            switch (testIndex){
                case 0:
                    test = new EmptyKeyValueTest();
                    break;
                case 1:
                    test = new KuduKeyValueTest();
                    break;
                case 2:
                    test = new DruidKeyValueTest();
                    break;
                case 3:
                    test = new CockroachKeyValueTest();
                    break;
                case 4:
                    test = new AlluxioKeyValueTest();
                    break;
                case 5:
                    test = new KafkaKeyValueTest();
                    break;

            }
            logger.info("Test:"+test.getClass().toString());
            logger.info(String.format("CreateData[size:%d,keyLength:%d,valueLength:%d]",dataSize,keyLength,valueLength));
            List<String> data = new ArrayList<String>();
            for (int i=0;i<dataSize;i++){
                String value = getRandomString(keyLength);
                data.add(value);
            }
            logger.info("Init:"+test.getClass().toString());
            test.init();
            logger.info("Store:"+dataSize);
            long storeStartTimestamp = System.currentTimeMillis();
            test.store(data);
            long storeEndTimestamp = System.currentTimeMillis();
            logger.info("Store Done:"+(storeEndTimestamp-storeStartTimestamp));
            logger.info("Scan:");
            long readStartTimestamp = System.currentTimeMillis();
            int count = test.scan();
            if(count!=dataSize){
                logger.info("count error");
            }
            long readEndTimestamp = System.currentTimeMillis();
            logger.info("Read Done:"+(readEndTimestamp-readStartTimestamp));
            logger.info("Destroy:"+test.getClass().toString());
            test.destroy();
            logger.info("Done:"+test.getClass().toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getRandomString(int length) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }
}
