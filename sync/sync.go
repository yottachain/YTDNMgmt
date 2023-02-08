package sync

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/embed"
	ebclient "github.com/aurawing/auramq/embed/cli"
	"github.com/aurawing/auramq/msg"
	"github.com/aurawing/auramq/ws"
	wsclient "github.com/aurawing/auramq/ws/cli"
	"github.com/golang/protobuf/proto"
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt/eostx"
	"github.com/yottachain/YTDNMgmt/pb"
)

//Service sync service
type Service struct {
	client     auramq.Client
	accountMap map[string]string
	master     *int32
	disableBP  bool
	//vid        int32
}

//StartSync start syncing
func StartSync(etx *eostx.EosTX, bindAddr string, routerBufferSize, subscriberBufferSize, readBufferSize, writerBufferSize, pingWait, readWait, writeWait int, topic string, snID int, urls, allowedAccounts []string, callback func(msg *msg.Message), shadowAccount, shadowPrivateKey string, isMaster *int32, disableBP bool) (*Service, error) {
	accountMap := make(map[string]string)
	for _, acc := range allowedAccounts {
		pk, err := etx.GetPublicKey(acc, "active")
		if err != nil {
			log.Printf("sync: StartSync: error when fetching public key of account %s: %s\n", acc, err)
			return nil, err
		}
		if strings.HasPrefix(pk, "YTA") {
			pk = string(pk[3:])
		}
		accountMap[acc] = pk
	}
	syncService := new(Service)
	syncService.master = isMaster
	syncService.accountMap = accountMap
	syncService.disableBP = disableBP
	//syncService.vid = vid

	router := auramq.NewRouter(routerBufferSize)
	go router.Run()

	wsbroker := ws.NewBroker(router, bindAddr, true, syncService.auth, subscriberBufferSize, readBufferSize, writerBufferSize, pingWait, readWait, writeWait)
	wsbroker.Run()

	ebbroker := embed.NewBroker(router, true, syncService.auth, subscriberBufferSize, subscriberBufferSize, subscriberBufferSize)
	ebbroker.Run()

	data := []byte(getRandomString(8))
	signature, err := ytcrypto.Sign(shadowPrivateKey, data)
	if err != nil {
		log.Printf("sync: StartSync: error when generating signature: %s\n", err)
		return nil, err
	}
	sigMsg := &pb.SignMessage{AccountName: shadowAccount, Data: data, Signature: signature}
	crendData, err := proto.Marshal(sigMsg)
	if err != nil {
		log.Printf("sync: StartSync: error when encoding SignMessage: %s\n", err)
		return nil, err
	}

	// curID := snID
	// if vid >= 0 {
	// 	curID = int(vid)
	// }
	for i, url := range urls {
		if i != snID {
			wsurl := url
			go func() {
				for {
					if atomic.LoadInt32(syncService.master) == 1 {
						cli, err := wsclient.Connect(wsurl, callback, &msg.AuthReq{Id: fmt.Sprintf("sn%d", snID), Credential: crendData}, []string{topic}, subscriberBufferSize, pingWait, readWait, writeWait)
						if err != nil {
							log.Printf("sync: StartSync: error when connecting to SN%d: %s\n", i, err)
							time.Sleep(time.Duration(3) * time.Second)
							continue
						}
						cli.Run()
						log.Printf("sync: StartSync: connect to SN%d successful\n", i)
					}
					time.Sleep(time.Duration(3) * time.Second)
				}
			}()
		} else {
			cli, err := ebclient.Connect(ebbroker.(*embed.Broker), callback, &msg.AuthReq{Id: fmt.Sprintf("sn%d", snID), Credential: crendData}, []string{topic})
			if err != nil {
				log.Printf("sync: StartSync: error when start syncing: %s\n", err)
				return nil, err
			}
			go func() {
				cli.Run()
			}()
			log.Println("sync: StartSync: create embeded broker successful")
			syncService.client = cli
		}
	}
	return syncService, nil
}

func (s *Service) auth(cred *msg.AuthReq) bool {
	if s.disableBP {
		return true
	}
	signMsg := new(pb.SignMessage)
	err := proto.Unmarshal(cred.Credential, signMsg)
	if err != nil {
		log.Printf("sync: auth: error when decoding SignMessage: %s\n", err)
		return false
	}
	if ytcrypto.Verify(s.accountMap[signMsg.AccountName], signMsg.Data, signMsg.Signature) {
		return true
	}
	return false
}

//Send one message to another client
func (s *Service) Send(to string, content []byte) bool {
	return s.client.Send(to, content)
}

//Publish one message
func (s *Service) Publish(topic string, content []byte) bool {
	return s.client.Publish(topic, content)
}

func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}
