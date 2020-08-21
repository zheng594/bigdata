package com.zheng.controller;

import com.zheng.config.WebSocket;
import com.zheng.model.SqlVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by zheng on 2020/4/23.
 */
@Slf4j
@Controller
@RequestMapping("web")
public class WebSocketController {

    @RequestMapping("/run")
    @ResponseBody
    public void run(@RequestBody SqlVO sqlVO) {
        WebSocket.sendMessage(sqlVO.getSql(), sqlVO.getSessionId());
    }
}
