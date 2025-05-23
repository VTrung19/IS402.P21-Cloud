package org.cloud.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.cloud.Constant.InvoiceItemFields;

import java.util.Date;

import static org.cloud.Constant.KAFKA_DATE_FORMAT;

public class InvoiceItem {

    @JsonProperty(InvoiceItemFields.TRANSACTION_ID)
    public String transactionId;

    @JsonProperty(InvoiceItemFields.CUSTOMER_ID)
    public String customId;

    @JsonProperty(InvoiceItemFields.TRANSACTION_TIME)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = KAFKA_DATE_FORMAT)
    public Date transactionTime;

    @JsonProperty(InvoiceItemFields.STORE)
    public String store;

    @JsonProperty(InvoiceItemFields.PRODUCT_ID)
    public String productId;

    @JsonProperty(InvoiceItemFields.TOTAL_PRICE)
    public double totalPrice;

    @JsonProperty(InvoiceItemFields.QUANTITY)
    public int quantity;
}