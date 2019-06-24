package com.alibaba.rocketmq.store;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MMAP:将硬盘某个文件的部分连续的内容或者全部内容映射到内存【连续的内存地址】里，
 * 对映射部分内容执行读写操作都是在内存里进行，至于修改过的内容何时刷盘到硬盘里取决于
 * 操作系统，只要操作系统不异常关机，修改的内容一定会持久化到硬盘的，异常宕机的话就会
 * 出现数据丢失。
 * 因为减少了复制次数所以性能高。
 *      常规传送一个文件到其他电脑的步骤【4次复制】：
 *          从硬盘读取数据到内核态内存，然后复制到用户态内存[jvm内存]，然后复制到内核态内存，然后复制到网卡，最后发送出去。
 *      mmap传送一个文件到其他电脑的步骤【2次复制】
 *          从硬盘读取数据到内存，然后复制到网卡，最后发送出去。
 *
 * 通过3种方式把1g的数据写入硬盘。
 */
public class TestMMap {
    private static final long Max_Size = Integer.MAX_VALUE;//1G大小
    public static void main(String[] args) throws Exception {
        String file1 = "D:\\tmp\\1.txt";
        String file2 = "D:\\tmp\\2.txt";
        String file3 = "D:\\tmp\\3.txt";
        write2fileByMapByteBuffer(file1);//最快
        //write2fileByFileOutputStream(file2);//慢的要死
        write2fileByWriteBuffer(file3);//几乎1分钟
    }
    public static void write2fileByFileOutputStream(String file) throws Exception {
        long start = System.currentTimeMillis();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        for(long i=0;i<Max_Size;i++){
            fileOutputStream.write((byte)'A');
        }
        fileOutputStream.close();
        long end = System.currentTimeMillis();
        System.out.println("write2fileByFileOutputStream:"+(end-start));
    }
    public static void write2fileByMapByteBuffer(String file) throws Exception {
        long start = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,Max_Size);
        for(long i=0;i<Max_Size;i++){
            mappedByteBuffer.put((byte)'A');
        }
        randomAccessFile.close();
        long end = System.currentTimeMillis();
        System.out.println("write2fileByMapByteBuffer:"+(end-start));
    }
    public static void write2fileByWriteBuffer(String file) throws Exception {
        long start = System.currentTimeMillis();
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
        for(long i=0;i<Max_Size;i++){
            bufferedWriter.write("A");
        }
        bufferedWriter.close();
        long end = System.currentTimeMillis();
        System.out.println("write2fileByMapByteBuffer:"+(end-start));
    }
}
