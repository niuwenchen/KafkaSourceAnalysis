package com.jackniu.kafka.common.requests;


import java.util.List;

public class BasePartitionState  {
    public final int controllerEpoch;
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;
    public final List<Integer> replicas;

    BasePartitionState(int controllerEpoch, int leader, int leaderEpoch, List<Integer> isr, int zkVersion, List<Integer> replicas) {
        this.controllerEpoch = controllerEpoch;
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
        this.replicas = replicas;
    }
}

