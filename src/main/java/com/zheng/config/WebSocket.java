package com.zheng.config;

import com.alibaba.fastjson.JSON;
import com.zheng.model.MessageVO;
import com.zheng.service.spark.SparkSqlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zheng on 2020/8/21
 */
@Slf4j
@Component
@ServerEndpoint("/websocket/{name}")
public class WebSocket {
    @PostConstruct
    public void init() {
        System.out.println("websocket 加载");
    }

    private static final AtomicInteger OnlineCount = new AtomicInteger(0);
    // concurrent包的线程安全Set，用来存放每个客户端对应的Session对象。
    private static CopyOnWriteArraySet<Session> SessionSet = new CopyOnWriteArraySet<Session>();


    /**
     * 连接建立成功调用的方法
     */
    @OnOpen
    public void onOpen(Session session) {
        SessionSet.add(session);
        sendMessage(session, "open", "");
    }

    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose(Session session) {
        SessionSet.remove(session);
        sendMessage(session, "close", "连接已关闭");
    }

    /**
     * 收到客户端消息后调用的方法
     *
     * @param message 客户端发送过来的消息
     */
    @OnMessage
    public void onMessage(String message, Session session) {
        sendMessage(session, "running", message);
    }

    /**
     * 出现错误
     *
     * @param session
     * @param error
     */
    @OnError
    public void onError(Session session, Throwable error) {
        log.error("发生错误：{}，Session ID： {}", error.getMessage(), session.getId());
    }

    public static void sendMessage(Session session, String status, String message) {
        sendMessage(session,status,message,null);
    }

        /**
         * 发送消息，实践表明，每次浏览器刷新，session会发生变化。
         *
         * @param session
         * @param message
         */
    public static void sendMessage(Session session, String status, String message,Object data) {
        try {
            MessageVO messageVO = new MessageVO();
            messageVO.setSessionId(session.getId());
            messageVO.setStatus(status);
            messageVO.setMessage(message);
            messageVO.setData(data);
            session.getBasicRemote().sendText(JSON.toJSONString(messageVO));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 群发消息
     *
     * @param message
     * @throws IOException
     */
    public static void broadCastInfo(String message) throws IOException {
        for (Session session : SessionSet) {
            if (session.isOpen()) {
                sendMessage(session, "running", message);
            }
        }
    }

    /**
     * 指定Session发送消息
     *
     * @param sessionId
     * @param message
     * @throws IOException
     */
    public static void run(String type,String message, String sessionId) {
        Session session = null;
        for (Session s : SessionSet) {
            if (s.getId().equals(sessionId)) {
                session = s;
                break;
            }
        }
        if (session != null) {
            try {
                Map<String,Object> data = SparkSqlJob.runJob(message);
                sendMessage(session, "running", "",data);
            } catch (Exception e) {
                sendMessage(session, "running", e.getMessage());
            }
        } else {
            log.error("会话找不到");
        }

    }

}
