package main

import (
	"log"
	"pg-Slave/client"

	"pg-Slave/conf"
	"time"
)

func main() {

	conf.Init()

	s, err := client.Connect()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// 创建新的CDC客户端
	err = s.DropReplicationSlot() // 如果存在，清理掉遗留的Slot
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer func(s *client.Slave) {
		err := s.DropReplicationSlot()
		if err != nil {
			log.Fatalln(err)
		}
	}(s) // 程序中止前清理掉复制槽
	err = s.CreateReplicationSlot() // 创建复制槽
	if err != nil {
		log.Fatalln(err)
		return
	}
	s.StartReplication() // 开始接收变更流
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err = s.ReportProgress()
			if err != nil {
				log.Fatalln(err)
			}
		}
	}() // 协程2每5秒地向主库汇报进度
	s.Subscribe() // 主消息循环
}
