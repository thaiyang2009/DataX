package com.alibaba.datax.plugin.writer.pulsarwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarWriter extends Writer {

    public static final String PARAMETER_SERVICE_URL = "serviceUrl";
    public static final String PARAMETER_TOPIC = "topic";
    public static final String PARAMETER_COLUMN = "column";
    public static final String PARAMETER_KEY_INDEX = "keyIndex";
    public static final String PARAMETER_PROPS = "props";

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            log.debug("kafka writer job conf:{}", this.conf.toJSON());
            this.conf.getNecessaryValue(PARAMETER_SERVICE_URL, PulsarWriterError.PULSAR_CONN_SERVICE_URL_MISSING);
            this.conf.getNecessaryValue(PARAMETER_TOPIC, PulsarWriterError.PULSAR_CONN_TOPIC_MISSING);
            this.conf.getNecessaryValue(PARAMETER_COLUMN, PulsarWriterError.PULSAR_COLUMN_MISSING);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.conf.clone());
            }
            return configList;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;
        private PulsarClient client;
        private Producer<String> producer;

        private String serviceUrl;
        private String topic;
        private List<String> columns;
        private int keyIndex = -1;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            serviceUrl = this.conf.getString(PARAMETER_SERVICE_URL);
            topic = this.conf.getString(PARAMETER_TOPIC);
            columns = this.conf.getList(PARAMETER_COLUMN, String.class);
            keyIndex = this.conf.getInt(PARAMETER_KEY_INDEX, -1);

            if (keyIndex >= columns.size()) {
                log.warn("key index out of range, keyIndex:{}", keyIndex);
                keyIndex = -1;
            }

            Map<String, Object> props = new HashMap<>();
            Map<String, Object> customizeProps = this.conf.getMap(PARAMETER_PROPS, Object.class);
            if (null != customizeProps) {
                props.putAll(customizeProps);
            }

            try {
                client = PulsarClient.builder()
                        .serviceUrl(serviceUrl)
                        .loadConf(props)
                        .build();
            } catch (PulsarClientException e) {
                throw DataXException.asDataXException(PulsarWriterError.PULSAR_CONN_SERVICE_URL_ERROR,
                        String.format("创建Pulsar客户端失败. serviceUrl: %s", serviceUrl), e);
            }

            try {
                producer = client.newProducer(Schema.STRING).topic(topic).create();
            } catch (PulsarClientException e) {
                throw DataXException.asDataXException(PulsarWriterError.PULSAR_CONN_BUILD_PRODUCER_ERROR,
                        String.format("创建Pulsar生产者失败. serviceUrl: %s, topic: %s", serviceUrl, topic), e);
            }

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start to writer kafka.");

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                String key = null;
                if (keyIndex >= 0) {
                    Column column = record.getColumn(keyIndex);
                    Object rawData = column.getRawData();
                    if (null != rawData) {
                        key = JSON.toJSONString(rawData);
                    }
                }

                String value = recordToJson(record);

                log.debug("send data. key: {}, value: {}", key, value);
                try {
                    producer.newMessage().key(key).value(value).send();
                } catch (PulsarClientException e) {
                    throw DataXException.asDataXException(PulsarWriterError.PULSAR_CONN_SEND_MESSAGE_ERROR,
                            String.format("发送消息失败. serviceUrl: %s, topic: %s", serviceUrl, topic), e);
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            if (producer != null) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    log.error("Pulsar生产者关闭失败", e);
                }
            }

            if (client != null) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    log.error("Pulsar客户端关闭失败", e);
                }
            }

        }

        private String recordToJson(Record record) {
            final int size = columns.size();
            final Map<String, String> data = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                final Column column = record.getColumn(i);
                data.put(columns.get(i), column.asString());
            }
            return JSON.toJSONString(data, JSONWriter.Feature.WriteMapNullValue);
        }
    }
}
