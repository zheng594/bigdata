package com.zheng.model;

import lombok.Data;

/**
 * Created by zheng on 2020/8/21
 */
@Data
public class MessageVO {
    private String sessionId;

    private String status;//open ,running,close

    private String message;
}
