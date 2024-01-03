//package com.zheng.config;
//
//import com.zheng.model.LogVO;
//import org.aspectj.lang.ProceedingJoinPoint;
//import org.aspectj.lang.annotation.Around;
//import org.aspectj.lang.annotation.Aspect;
//import org.aspectj.lang.annotation.Pointcut;
//import org.aspectj.lang.reflect.MethodSignature;
//import org.mortbay.util.ajax.JSON;
//import org.springframework.stereotype.Component;
//
//import java.lang.reflect.Method;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
///**
// * Created by zheng on 2020/8/28
// */
//@Aspect
//@Component
//public class LogAspect {
//    /**
//     * 这里我们使用注解的形式
//     * 当然，我们也可以通过切点表达式直接指定需要拦截的package,需要拦截的class 以及 method
//     * 切点表达式:   execution(...)
//     */
//    @Pointcut("@annotation(com.space.aspect.anno.SysLog)")
//    public void logPointCut() {
//    }
//
//    /**
//     * 环绕通知 @Around  ， 当然也可以使用 @Before (前置通知)  @After (后置通知)
//     * @param point
//     * @return
//     * @throws Throwable
//     */
//    @Around("logPointCut()")
//    public Object around(ProceedingJoinPoint point) throws Throwable {
//        long beginTime = System.currentTimeMillis();
//        Object result = point.proceed();
//        long time = System.currentTimeMillis() - beginTime;
//        try {
//            saveLog(point, time);
//        } catch (Exception e) {
//        }
//        return result;
//    }
//
//    /**
//     * 保存日志
//     * @param point
//     * @param runTime
//     */
//    private void saveLog(ProceedingJoinPoint point, long runTime) {
//        MethodSignature signature = (MethodSignature) point.getSignature();
//        Method method = signature.getMethod();
//        LogVO logVO = new LogVO();
//        logVO.setRunTime(runTime);
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//        logVO.setAccessTime(dateFormat.format(new Date()));
//        SysLog sysLog = method.getAnnotation(SysLog.class);
//        if (sysLog != null) {
//            //注解上的描述
//            logVO.setRemark(sysLog.value());
//        }
//        //请求的 类名、方法名
//        String className = point.getTarget().getClass().getName();
//        String methodName = signature.getName();
//        logVO.setClassName(className);
//        logVO.setMethodName(methodName);
//        //请求的参数
//        Object[] args = point.getArgs();
//        List<String> list = new ArrayList<String>();
//        for (Object o : args) {
//            list.add(JSON.toString(point));
//        }
//        LogVO.setParams(list.toString());
//    }
//}
