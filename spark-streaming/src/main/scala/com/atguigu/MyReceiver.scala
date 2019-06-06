package com.atguigu

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


/*
从一个网络端口接收数据:  hadoop201  20000
 */
class MyReceiver(val host: String, val port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    override def onStart(): Unit = {
        new Thread() {
            override def run(): Unit = receive()
        }.start()
    }
    
    def receive() = {
        var socket: Socket = null
        var reader: BufferedReader = null
        try {
            // 网络编程
            // 1. 创建socket
            socket = new Socket(host, port)
            // 2. 从socket去读取数据
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
            // 3. 读取到数据, 然后存储
            var line: String = reader.readLine()
            while (line != null && !isStopped()) {
                store(line)
                line = reader.readLine()
            }
        } catch {
            case e =>
            
        } finally {
            
            restart("again")
        }
    }
    
    /**
      * 将来接收器停止工作的时候, 做的工作: 释放资源
      */
    override def onStop(): Unit = {
    
    }
    
}
