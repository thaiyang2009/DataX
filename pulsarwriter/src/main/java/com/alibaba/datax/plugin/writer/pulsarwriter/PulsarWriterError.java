package com.alibaba.datax.plugin.writer.pulsarwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author tanghaiyang
 */
public enum PulsarWriterError implements ErrorCode {
    PULSAR_CONN_SERVICE_URL_MISSING("PULSAR_CONN_SERVICE_URL_MISSING", "缺少serviceUrl配置"),
    PULSAR_CONN_TOPIC_MISSING("PULSAR_CONN_TOPIC_MISSING", "缺少topic配置"),
    PULSAR_CONN_SERVICE_URL_ERROR("PULSAR_CONN_SERVICE_URL_ERROR", "创建Pulsar客户端失败，请检查serviceUrl配置是否正确"),
    PULSAR_CONN_BUILD_PRODUCER_ERROR("PULSAR_CONN_BUILD_PRODUCER_ERROR", "创建Pulsar生产者失败，请检查topic字段配置是否正确"),
    PULSAR_COLUMN_MISSING("PULSAR_COLUMN_MISSING", "缺少column配置"),
    PULSAR_CONN_SEND_MESSAGE_ERROR("PULSAR_CONN_SEND_MESSAGE_ERROR", "发送消息失败"),
    ;

    private final String code;
    private final String desc;

    PulsarWriterError(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
