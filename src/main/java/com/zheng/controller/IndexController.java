package com.zheng.controller;

import com.zheng.model.Result;
import com.zheng.model.SqlVO;
import com.zheng.spark.SparkSqlJob;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by zheng on 2020/4/23.
 */
@Controller
public class IndexController {
    @RequestMapping("/")
    public String index() {
        return "page/edit";
    }

    @RequestMapping("/querySql")
    @ResponseBody
    public Result querySql(@RequestBody SqlVO sqlVO) {
        Row[] rows = null;
        try {
            rows =  SparkSqlJob.runJob(sqlVO.getSql());
        } catch (Exception e) {
            return Result.failureResult(e.getMessage());
        }
        return Result.successResult(rows.toString());
    }
}
