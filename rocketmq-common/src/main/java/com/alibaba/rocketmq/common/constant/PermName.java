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
package com.alibaba.rocketmq.common.constant;

/**
 * 权限
 *     针对二进制来说的，一位代表一种权限，1代表有此种权限，0代表无此种权限
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PermName {
    public static final int PERM_PRIORITY = 0x1 << 3;//8 -》 1000
    public static final int PERM_READ = 0x1 << 2;//4 -》 0100
    public static final int PERM_WRITE = 0x1 << 1;//2 -》 0010
    public static final int PERM_INHERIT = 0x1 << 0;//1 -》 0001


    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }


    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }


    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }


    public static String perm2String(final int perm) {
        final StringBuffer sb = new StringBuffer("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        int basePerm = 0x1;
        System.out.println(basePerm);
        System.out.println("PERM_PRIORITY:"+PERM_PRIORITY+"-"+Integer.toBinaryString(PERM_PRIORITY));
        System.out.println("PERM_READ:"+PERM_READ+"-"+Integer.toBinaryString(PERM_READ));
        System.out.println("PERM_WRITE:"+PERM_WRITE+"-"+Integer.toBinaryString(PERM_WRITE));
        System.out.println("PERM_INHERIT:"+PERM_INHERIT+"-"+Integer.toBinaryString(PERM_INHERIT));
        System.out.println("--------------------------------");
        //具有读写权限
        int initPerm  = PERM_READ | PERM_WRITE;
    }

    /**
     * 增加权限
     * @param currentPerm 当前权限
     * @param addPerm 新增一个权限
     * @return
     */
    public static int  addPerm(int currentPerm,int addPerm){
        return currentPerm | addPerm;
    }

    /**
     * 判断是否具有某一种权限
     * @param currentPerm
     * @param hasPerm 一种权限
     * @return
     */
    public static boolean hasPerm(int currentPerm,int hasPerm){
        return (currentPerm & hasPerm) == hasPerm;
    }

    /**
     * 剔除某一种权限
     * @param currentPerm
     * @param delPerm 要删除的一种权限
     * @return
     */
    public static int delPerm(int currentPerm,int delPerm){
        return currentPerm & (~delPerm);
    }
}
