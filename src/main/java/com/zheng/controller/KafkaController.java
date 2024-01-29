package com.zheng.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author zheng
 * @Date 2024/1/3
 **/
@Slf4j
@RestController
public class KafkaController {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("send")
    public String send() {
        for (int i = 0; i < 100; i++) {
            String str ="这是第"+i+"条消息";
            kafkaTemplate.send("test-topic",  str);
        }
        return "success";
    }

    // 消费监听
    @KafkaListener(topics = {"test-topic"})
    public void onMessage(ConsumerRecord<?, ?> record){
       log.error("简单消费："+record.topic()+"-"+record.partition()+"-"+record.value());
    }
}
