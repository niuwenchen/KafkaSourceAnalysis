package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.errors.ApiException;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Struct;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;
import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_MESSAGE;

public class ApiError {
    public static final ApiError NONE = new ApiError(Errors.NONE, null);

    private final Errors error;
    private final String message;

    public static ApiError fromThrowable(Throwable t) {
        // Avoid populating the error message if it's a generic one
        Errors error = Errors.forException(t);
        String message = error.message().equals(t.getMessage()) ? null : t.getMessage();
        return new ApiError(error, message);
    }

    public ApiError(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        // In some cases, the error message field was introduced in newer version
        message = struct.getOrElse(ERROR_MESSAGE, null);
    }

    public ApiError(Errors error, String message) {
        this.error = error;
        this.message = message;
    }

    public void write(Struct struct) {
        struct.set(ERROR_CODE, error.code());
        if (error != Errors.NONE)
            struct.setIfExists(ERROR_MESSAGE, message);
    }

    public boolean is(Errors error) {
        return this.error == error;
    }

    public boolean isFailure() {
        return !isSuccess();
    }

    public boolean isSuccess() {
        return is(Errors.NONE);
    }

    public Errors error() {
        return error;
    }

    /**
     * Return the optional error message or null. Consider using {@link #messageWithFallback()} instead.
     */
    public String message() {
        return message;
    }

    /**
     * If `message` is defined, return it. Otherwise fallback to the default error message associated with the error
     * code.
     */
    public String messageWithFallback() {
        if (message == null)
            return error.message();
        return message;
    }

    public ApiException exception() {
        return error.exception(message);
    }

    @Override
    public String toString() {
        return "ApiError(error=" + error + ", message=" + message + ")";
    }
}
