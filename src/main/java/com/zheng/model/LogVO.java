package com.zheng.model;

import lombok.Data;

/**
 * Created by zheng on 2020/8/28
 */
@Data
public class LogVO {
    private String accessTime;
    private long runTime;
    private String className;
    private String methodName;
    private String params;
    private String remark;
}
