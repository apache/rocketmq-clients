package org.apache.rocketmq.admin;

import static apache.rocketmq.v1.AdminGrpc.getChangeLogLevelMethod;

import apache.rocketmq.v1.AdminGrpc;
import apache.rocketmq.v1.ChangeLogLevelRequest;
import apache.rocketmq.v1.ChangeLogLevelResponse;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

public class AdminService extends AdminGrpc.AdminImplBase {

    @Override
    public void changeLogLevel(ChangeLogLevelRequest request, StreamObserver<ChangeLogLevelResponse> responseObserver) {
        ServerCalls.asyncUnimplementedUnaryCall(getChangeLogLevelMethod(), responseObserver);
    }

}
