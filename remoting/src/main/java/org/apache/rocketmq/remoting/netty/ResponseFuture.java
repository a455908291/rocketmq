/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ResponseFuture {
    // requestId
    private final int opaque;
    // 请求发送出去的网络连接
    private final Channel processChannel;
    // 请求等待响应的超时时间
    private final long timeoutMillis;
    // async rpc调用响应invoke
    private final InvokeCallback invokeCallback;
    // 请求开始时间戳
    private final long beginTimestamp = System.currentTimeMillis();
    // 用于进行并发控制的countDownLatch
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    // 行为控制的一个标识， 仅仅支持释放一次的semaphore的组件的封装
    private final SemaphoreReleaseOnlyOnce once;
    // 支持仅仅执行一次的callback调用的CAS组件
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    // 发起的远程调用的请求命令对应的响应
    private volatile RemotingCommand responseCommand;
    // 这次请求的发送是否成功
    private volatile boolean sendRequestOK = true;
    // 这次的rpc调用遇到的异常
    private volatile Throwable cause;

    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback,
        SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    // 如果响应回来以后， 执行invoke回调
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    // 释放信号
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    // 判断当前请求是否超时
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    // 等待请求响应
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    // 处理响应
    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand
            + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause
            + ", opaque=" + opaque
            + ", processChannel=" + processChannel
            + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback
            + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
