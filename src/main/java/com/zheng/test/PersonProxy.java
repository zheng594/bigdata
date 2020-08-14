package com.zheng.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * Created by zheng on 2020/6/30.
 */
public class PersonProxy {
    private Object target;

    private InvocationHandler handler;

    public PersonProxy(Object target,InvocationHandler handler){
        this.target = target;
        this.handler = handler;
    }

    public Object getPersonProxy() {
        Object p = Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                target.getClass().getInterfaces(),
                handler
        );
        return p;
    }

}
