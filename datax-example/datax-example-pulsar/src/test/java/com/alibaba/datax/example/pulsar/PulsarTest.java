package com.alibaba.datax.example.pulsar;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;


public class PulsarTest {
    @Test
    public void testStreamReader2KafkaWriter() {
        String path = "/stream2pulsar.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
