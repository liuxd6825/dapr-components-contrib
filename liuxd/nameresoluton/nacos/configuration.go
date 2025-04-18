package nacos

import (
	"encoding/json"
	nr "github.com/dapr/components-contrib/nameresolution"
)

type resolverConfig struct {
	Server       []*ServerConfig       `json:"server"`
	Client       *ClientConfig         `json:"client"`
	Selected     *SelectedServerConfig `json:"selected"`
	Registration RegistrationConfig    `json:"registration"`
}

type ServerConfig struct {
	IP     string `json:"ip"`
	Port   uint64 `json:"port"`
	Path   string `json:"path"`
	Scheme string `json:"scheme"`
}

type ClientConfig struct {
	TimeoutMs            uint64            `json:"timeoutMs"`            // timeout for requesting Nacos server, default value is 10000ms
	ListenInterval       uint64            `json:"listenInterval"`       // Deprecated
	BeatInterval         int64             `json:"beatInterval"`         // the time interval for sending beat to server,default value is 5000ms
	NamespaceId          string            `json:"namespaceId"`          // the namespaceId of Nacos.When namespace is public, fill in the blank string here.
	AppName              string            `json:"appName"`              // the appName
	AppKey               string            `json:"appKey"`               // the client identity information
	Endpoint             string            `json:"endpoint"`             // the endpoint for get Nacos server addresses
	RegionId             string            `json:"regionId"`             // the regionId for kms
	AccessKey            string            `json:"accessKey"`            // the AccessKey for kms
	SecretKey            string            `json:"secretKey"`            // the SecretKey for kms
	OpenKMS              bool              `json:"openKMS"`              // it's to open kms, default is false. https://help.aliyun.com/product/28933.html
	CacheDir             string            `json:"cacheDir"`             // the directory for persist nacos service info,default value is current path
	DisableUseSnapShot   bool              `json:"disableUseSnapShot"`   // It's a switch, default is false, means that when get remote config fail, use local cache file instead
	UpdateThreadNum      int               `json:"updateThreadNum"`      // the number of goroutine for update nacos service info,default value is 20
	NotLoadCacheAtStart  bool              `json:"notLoadCacheAtStart"`  // not to load persistent nacos service info in CacheDir at start time
	UpdateCacheWhenEmpty bool              `json:"updateCacheWhenEmpty"` // update cache when get empty service instance from server
	Username             string            `json:"username"`             // the username for nacos auth
	Password             string            `json:"password"`             // the password for nacos auth
	LogDir               string            `json:"logDir"`               // the directory for log, default is current path
	LogLevel             string            `json:"logLevel"`             // the level of log, it's must be debug,info,warn,error, default value is info
	ContextPath          string            `json:"contextPath"`          // the nacos server contextpath
	AppendToStdout       bool              `json:"appendToStdout"`       // if append log to stdout
	AsyncUpdateService   bool              `json:"asyncUpdateService"`   // open async update service by query
	EndpointContextPath  string            `json:"endpointContextPath"`  // the address server  endpoint contextPath
	EndpointQueryParams  string            `json:"endpointQueryParams"`  // the address server  endpoint query params
	ClusterName          string            `json:"clusterName"`          // the address server  clusterName
	AppConnLabels        map[string]string `json:"appConnLabels"`        // app conn labels

	/*
		LogSampling          *ClientLogSamplingConfig // the sampling config of log
		LogRollingConfig     *ClientLogRollingConfig  // log rolling config
		TLSCfg               TLSConfig                // tls Config
		RamConfig            *RamConfig
		KMSVersion           KMSVersion   // kms client version. https://help.aliyun.com/document_detail/380927.html
		KMSv3Config          *KMSv3Config //KMSv3 configuration. https://help.aliyun.com/document_detail/601596.html
		KMSConfig            *KMSConfig
	*/
}

type SelectedServerConfig struct {
	Clusters    []string `json:"clusters"`    //optional
	ServiceName string   `json:"serviceName"` //required
	GroupName   string   `json:"groupName"`   //optional,default:DEFAULT_GROUP
	HealthyOnly bool     `json:"healthyOnly"` //optional,value = true return only healthy instance, value = false return only unHealthy instance
}

type RegistrationConfig struct {
	Weight      float64           `json:"weight"`      //required,it must be lager than 0
	Enable      bool              `json:"enable"`      //required,the instance can be access or not
	Healthy     bool              `json:"healthy"`     //required,the instance is health or not
	Metadata    map[string]string `json:"metadata"`    //optional
	ClusterName string            `json:"clusterName"` //optional
	ServiceName string            `json:"serviceName"` //required
	GroupName   string            `json:"groupName"`   //optional,default:DEFAULT_GROUP
	Ephemeral   bool              `json:"ephemeral"`   //optional
}

func NewResolverConfig(metadata nr.Metadata) *resolverConfig {
	cfg := &resolverConfig{}
	data, err := json.Marshal(metadata.Configuration)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(data, cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}
