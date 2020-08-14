package com.zheng.model;

import lombok.Data;

/**
 * Created by zheng on 2019-09-19.
 */
@Data
public class Result<R> {
    private String message;

    private boolean success;

    private R data;

    public Result() {
    }

    public Result(boolean success, String message, R data) {
        this.success = success;
        this.message = message;
        this.data = data;
    }

    public static <R> Result<R> failureResult(String message, R data) {
        return new Result<R>(false, message, data);
    }

    public static <R> Result<R> failureResult(String message) {
        return new Result<R>(false, message, null);
    }

    public static <R> Result<R> failureResult(R data) {
        return new Result<R>(false, null, data);
    }

    public static Result<Void> failureResult() {
        return new Result<Void>(false, null, null);
    }

    public static <R> Result<R> successResult(String message, R data) {
        return new Result<R>(true, message, data);
    }

    public static <R> Result<R> successResult(R data) {
        return new Result<R>(true, null, data);
    }

    public static Result<Void> successResult() {
        return new Result<Void>(true, null, null);
    }

}
