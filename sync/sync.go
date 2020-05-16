package sync

import (
	"fmt"
	"log"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/embed"
	ebclient "github.com/aurawing/auramq/embed/cli"
	"github.com/aurawing/auramq/msg"
	"github.com/aurawing/auramq/ws"
	wsclient "github.com/aurawing/auramq/ws/cli"
)

//Service sync service
type Service struct {
	client auramq.Client
}

//StartSync start syncing
func StartSync(bindAddr string, routerBufferSize, subscriberBufferSize, readBufferSize, writerBufferSize, pingWait, readWait, writeWait int, topic string, snID int, urls []string, callback func(msg *msg.Message)) (*Service, error) {
	router := auramq.NewRouter(routerBufferSize)
	go router.Run()

	wsbroker := ws.NewBroker(router, bindAddr, false, nil, subscriberBufferSize, readBufferSize, writerBufferSize, pingWait, readWait, writeWait)
	wsbroker.Run()

	ebbroker := embed.NewBroker(router, false, nil, subscriberBufferSize, subscriberBufferSize, subscriberBufferSize)
	ebbroker.Run()

	syncService := new(Service)
	for i, url := range urls {
		if i != snID {
			wsurl := url
			go func() {
				for {
					cli, err := wsclient.Connect(wsurl, callback, &msg.AuthReq{Id: fmt.Sprintf("sn%d", snID), Credential: []byte("welcome")}, []string{topic}, subscriberBufferSize, pingWait, readWait, writeWait)
					if err != nil {
						log.Printf("sync: StartSync: error when connecting to SN%d: %s\n", i, err)
						time.Sleep(time.Duration(3) * time.Second)
						continue
					}
					cli.Run()
					time.Sleep(time.Duration(3) * time.Second)
				}
			}()
		} else {
			cli, err := ebclient.Connect(ebbroker.(*embed.Broker), callback, &msg.AuthReq{Id: fmt.Sprintf("sn%d", snID), Credential: []byte("welcome")}, []string{topic})
			if err != nil {
				log.Printf("sync: StartSync: error when start syncing: %s\n", err)
				return nil, err
			}
			go func() {
				cli.Run()
			}()
			syncService.client = cli
		}
	}
	return syncService, nil
}

//Send one message to another client
func (s *Service) Send(to string, content []byte) bool {
	return s.client.Send(to, content)
}

//Publish one message
func (s *Service) Publish(topic string, content []byte) bool {
	return s.client.Publish(topic, content)
}
