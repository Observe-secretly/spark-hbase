package cn.tsign.common.druid.util;

import java.io.Serializable;

import com.google.gson.Gson;

public class DruidTaskInfo implements Serializable {

    private static final long serialVersionUID = 3438117490904888452L;

    private String            path;

    private String            taskId;

    private String            status;

    /**
     * 除了提交失败外的其他异常，异常信息会被写入message中
     */
    private boolean           isError          = false;

    private String            errorMessage;

    public DruidTaskInfo(String path){
        this.path = path;
    }

    public boolean isError() {
        return isError;
    }

    public void setError(boolean isError) {
        this.isError = isError;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
