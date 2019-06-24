/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;


/**
 * 引用计数基类，类似于C++智能指针实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public abstract class ReferenceResource {
    /**
     * 引用计数，大于0资源可用未被shutdown[可用available=true]，小于等于0可以回收[不可用available=false]
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    /**
     * 是否可用
     */
    protected volatile boolean available = true;
    /**
     * 是否清理干净
     */
    protected volatile boolean cleanupOver = false;
    /**
     * 第一次调用shutdown方法的时间戳
     */
    private volatile long firstShutdownTimestamp = 0;


    /**
     * 资源是否能HOLD住
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            }
            else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }


    /**
     * 资源是否可用，即是否可被HOLD
     */
    public boolean isAvailable() {
        return this.available;
    }


    /**
     * 禁止资源被访问 shutdown不允许调用多次，最好是由管理线程调用
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        }
        // 强制shutdown
        else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }


    public long getRefCount() {
        return this.refCount.get();
    }


    /**
     * 释放资源
     */
    public void release() {
        long value = this.refCount.decrementAndGet();//引用数减1
        if (value > 0)//引用数还是大于0则资源不释放
            return;

        synchronized (this) {//引用数小于等于0资源可释放，并将执行资源清理方法
            // cleanup内部要对是否clean做处理
            this.cleanupOver = this.cleanup(value);
        }
    }

    /**
     * 资源清理程序由子类实现
     * @param currentRef
     * @return
     */
    public abstract boolean cleanup(final long currentRef);


    /**
     * 资源是否被清理完成
     *      1，引用数小于等于0，代表可以执行清理工作
     *      2，cleanupOver=true,代表清理工作执行过
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
