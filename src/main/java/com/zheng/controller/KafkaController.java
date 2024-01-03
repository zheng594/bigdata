package com.zheng.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author zheng
 * @Date 2024/1/3
 **/
@RestController
public class KafkaController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("send")
    public String send() {
        for (int i = 0; i < 100; i++) {
            String str ="这是第"+i+"条消息";
            kafkaTemplate.send("test_topic",  str);
        }
        return "success";
    }
}
