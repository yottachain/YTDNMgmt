# YTDNMgmt
## 1. 打包与部署
本次更新仍然采用嵌入式打包的方式，与之前的方式相同，即将`YTDNMgmt`编译为共享库嵌入至`YTDNMgmtJavaBinding`项目中，但是与SN的交互采用了和P2PHost类似的GRPC方式。

## 2. 配置方式
原有的基于环境变量的程序配置方式改为配置文件，文件名为`nodemgmt.yaml`，放置在`/app/ytsn/conf`目录下，内容如下：

```
#GRPC绑定地址，默认为0.0.0.0:11001
grpc-bind-addr: ":11001"
#PProf配置
pprof:
  #是否开启PProf
  enable: true
  #绑定地址，默认为0.0.0.0:6161
  bind-addr: ":6161"
#消息队列配置
auramq:
  #绑定地址，默认为0.0.0.0:8787
  bind-addr: ":8787"
  #消息路由缓冲区大小，默认1024
  router-buffer-size: 1024
  #消息订阅者缓冲区大小，默认1024
  subscriber-buffer-size: 1024
  #读缓冲区大小，默认4KB
  read-buffer-size: 4096
  #写缓冲区大小，默认4KB
  write-buffer-size: 4096
  #client与server之间发送ping消息的时间间隔，默认30秒
  ping-wait: 30
  #消息读超时，默认为60秒
  read-wait: 60
  #消息写超时，默认为10秒
  write-wait: 10
  #用于矿机信息同步的主题名称，默认为sync
  miner-sync-topic: "sync"
  #所有SN暴露的消息队列监听地址，需按SN编号依次配置
  all-sn-urls:
  - "ws://172.17.0.2:8787/ws"
  - "ws://172.17.0.3:8787/ws"
  - "ws://172.17.0.4:8787/ws"
  #用于鉴权的BP账号，需配置为全部SN的shadow账号
  allowed-accounts:
  - shadow1
  - shadow2
  - shadow3
#杂项配置
misc:
  #允许以该参数值为前缀的内网地址为可用地址，一般用于内网测试环境，默认为空
  exclude-addr-prefix: "/ip4/172.17"
  #生产空间与使用空间的差值小于该值会触发预采购，默认为500MB
  pre-purchase-threshold: 32768
  #每次预采购的空间大小，默认为1GB
  pre-purchase-amount: 65536
  #相邻两次上报时间小于该值被认为是可用矿机，默认为3分钟
  avaliable-miner-time-gap: 3
  #刷新可分配矿机列表的时间间隔，默认为10分钟
  miner-alloc-refresh-interval: 10
  #刷新矿池权重表的时间间隔，默认为10分钟
  pool-weight-refresh-interval: 10
  #可用矿机版本号阈值，版本号小于该值的矿机被认为不可用，默认为1
  miner-version-threshold: 1
  #执行连通性测试时的连接超时时间，默认为10秒
  connectivity-connect-timeout: 10
  #执行连通性测试时的读超时时间，默认为10秒
  connectivity-read-timeout: 10
  #超过该时间没有上报的矿机被认为是失效矿机并作为参数用于计算矿池错误率，默认为4小时
  pool-error-miner-time-threshold: 14400
  #矿池错误率小于该值时矿池内的矿机不被惩罚，默认为95%
  pool-error-miner-percentage-threshold: 95
  #连通性测试时间间隔，默认为1小时
  connectivity-test-interval: 60
  #是否启用测试模式，默认为false
  enable-test: false
  #是否在权重计算中使用实验性参数，默认为true
  enable-extra-weight-params: true
  #从BP同步抵押和生产空间的时间间隔，默认为60（分钟）
  bp-sync-interval: 60
  #IP数据库的位置
  ipdb-path: "/app/ytsn/yotta.ipdb"
```
最大的变化来自于SN之间同步矿机数据的方式，之前采用SN向其他SN推的方式，本次架构调整改为从其他SN拉取的方式，这样可以保证矿机数据的实时性，并且可以支持第三方服务从SN及时获取矿机信息。相关的配置集中在`auramq`配置节内，由于消息通信基于websocket实现，因此`all-sn-urls`中各SN的接入点均为websocket的URL格式（`ws://<IP>:<PORT>/ws`）,比如主网有21个SN，那在这里需要依次配置全部21个SN的消息监听地址（包括当前SN在内）

## 3. 测试事项
测试时主要注意观察矿机之间的信息同步是否稳定，是否有延时，另外还需要观察其他功能是否正常，是否有兼容性问题