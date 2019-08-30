package cn.tsign.entity;

import java.io.Serializable;

public class Rt implements Serializable {

    private static final long serialVersionUID = -5570219549772046358L;

    private long              startTime;

    private long              endTime;

    public Rt(){
    }

    public Rt(long startTime, long endTime){
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getDaly() {
        return endTime - startTime;
    }

}
