package conf

import "perf-storage-go/util"

var (
	RedisCluster  = util.GetEnvBool("REDIS_CLUSTER", false)
	RedisAddr     = util.GetEnvStr("REDIS_ADDR", "localhost:9000")
	RedisPassword = util.GetEnvStr("REDIS_PASSWORD", "")
)
