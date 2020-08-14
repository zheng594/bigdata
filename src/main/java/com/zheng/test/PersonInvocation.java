package com.zheng.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Created by zheng on 2020/6/30.
 */
public class PersonInvocation implements InvocationHandler {
    private Object target;

    public PersonInvocation(Object target) {
        this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("修改个人信息前记录日志");
        method.invoke(target);
        System.out.println("修改个人信息后记录日志");
        return null;
    }
}
