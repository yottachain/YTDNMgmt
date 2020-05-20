# YTDNMgmt

```
grpc-bind-addr: ":11001"
pprof:
  enable: true
  bind-addr: ":6161"
auramq:
  bind-addr: ":8787"
  router-buffer-size: 1024
  subscriber-buffer-size: 1024
  read-buffer-size: 4096
  write-buffer-size: 4096
  ping-wait: 30
  read-wait: 60
  write-wait: 10
  miner-sync-topic: "sync"
  all-sn-urls:
  - "ws://172.17.0.2:8787/ws"
  - "ws://172.17.0.3:8787/ws"
  - "ws://172.17.0.4:8787/ws"
  allowed-accounts:
  - shadow1
  - shadow2
  - shadow3
misc:
  exclude-addr-prefix: "/ip4/172.17"
  pre-purchase-threshold: 32768
  pre-purchase-amount: 65536
  avaliable-miner-time-gap: 3
  miner-alloc-refresh-interval: 10
  pool-weight-refresh-interval: 10
  miner-version-threshold: 1
  connectivity-connect-timeout: 10
  connectivity-read-timeout: 10
  pool-error-miner-time-threshold: 14400
  pool-error-miner-percentage-threshold: 95
  connectivity-test-interval: 60
  enable-test: true
  enable-extra-weight-params: true
  ipdb-path: "/app/ytsn/yotta.ipdb"
```