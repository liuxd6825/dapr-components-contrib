package nacos

import (
	"context"
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/utils"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	consul "github.com/hashicorp/consul/api"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"math/rand"
	"sync"
	"time"
)

const (
	nacosServerAddr = "serverAddress"
	nacosNamespace  = "namespace"
)

type client struct {
	naming_client.INamingClient
}

func (c *client) InitClient(cfg *vo.NacosClientParam) error {
	// create naming client
	newClient, err := clients.NewNamingClient(
		*cfg,
	)
	c.INamingClient = newClient
	return err
}

type resolver struct {
	client             *client
	logger             logger.Logger
	registry           *registry
	metadata           nr.Metadata
	cfg                *resolverConfig
	watcherStopChannel chan struct{}
}

type registryEntry struct {
	services []*consul.ServiceEntry
	mu       sync.RWMutex
}

type registryInterface interface {
	getKeys() []string
	get(service string) *registryEntry
	expire(service string) // clears slice of instances
	expireAll()            // clears slice of instances for all entries
	remove(service string) // removes entry from registry
	removeAll()            // removes all entries from registry
	addOrUpdate(service string, services []*consul.ServiceEntry)
	registrationChannel() chan string
}

type registry struct {
	entries        sync.Map
	serviceChannel chan string
}

type clientInterface interface {
	InitClient(cfg *vo.NacosClientParam) error
}

// NewResolver creates Consul name resolver.
func NewResolver(logger logger.Logger) nr.Resolver {
	return newResolver(logger, &resolverConfig{}, &client{}, &registry{serviceChannel: make(chan string, 100)}, make(chan struct{}))
}

func newResolver(logger logger.Logger, cfg *resolverConfig, client *client, registry *registry, watcherStopChannel chan struct{}) nr.Resolver {
	return &resolver{
		logger:             logger,
		cfg:                cfg,
		client:             client,
		registry:           registry,
		watcherStopChannel: watcherStopChannel,
	}
}

func (r *resolver) Init(ctx context.Context, metadata nr.Metadata) error {
	r.metadata = metadata
	r.cfg = NewResolverConfig(metadata)
	err := r.client.InitClient(r.newNacosClientParam())
	if err != nil {
		return err
	}
	regParam := r.newRegisterInstance()
	ok, err := r.client.RegisterInstance(*regParam)
	if err != nil {
		return err
	}
	if !ok {
		panic("failed to register instance to nacos")
	}
	return err
}

// ResolveID 实现 Dapr NameResolver 接口
func (r *resolver) ResolveID(ctx context.Context, req nr.ResolveRequest) (string, error) {
	param := r.newSelectInstances(ctx, req)
	instances, err := r.client.SelectInstances(*param)
	if err != nil {
		return "", err
	}
	if len(instances) == 0 {
		return "", errors.New("nacos SelectInstances() no instances found")
	}
	return convertToDaprInstances(instances), nil
}

func (r *resolver) Close() (err error) {
	defer func() {
		err = utils.GetRecoverError(err, recover())
	}()
	deParma := r.newDeregisterInstanceParam()
	for i := 0; i < 3; i++ {
		ok, err := r.client.DeregisterInstance(*deParma)
		if err != nil || !ok {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	r.client.CloseClient()
	return err
}

// 实例对象转换
func convertToDaprInstances(instances []model.Instance) (address string) {
	// 实现随机负载均衡
	rand.Seed(time.Now().UnixNano())
	selected := instances[rand.Intn(len(instances))]
	address = fmt.Sprintf("%s:%d", selected.Ip, selected.Port)
	return address
}

func (r *resolver) newNacosClientParam() *vo.NacosClientParam {
	//create ServerConfig
	sc := []constant.ServerConfig{}
	for _, item := range r.cfg.Server {
		serverCfg := *constant.NewServerConfig(item.IP, item.Port, constant.WithContextPath(item.Path))
		sc = append(sc, serverCfg)
	}

	//create ClientConfig
	cc := *constant.NewClientConfig()
	return &vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	}
}

func (r *resolver) newDeregisterInstanceParam() *vo.DeregisterInstanceParam {
	param := &vo.DeregisterInstanceParam{
		Ip:          r.metadata.Instance.Address,
		ServiceName: r.metadata.Instance.AppID,
		Port:        uint64(r.metadata.Instance.DaprHTTPPort),
		GroupName:   r.cfg.Registration.GroupName,
		Ephemeral:   r.cfg.Registration.Ephemeral,
	}
	return param
}

func (r *resolver) newRegisterInstance() *vo.RegisterInstanceParam {
	param := &vo.RegisterInstanceParam{
		Ip:          r.metadata.Instance.Address,
		ServiceName: r.metadata.Instance.AppID,
		Port:        uint64(r.metadata.Instance.DaprHTTPPort),
		Weight:      r.cfg.Registration.Weight,
		Enable:      r.cfg.Registration.Enable,
		Healthy:     r.cfg.Registration.Healthy,
		Metadata:    r.cfg.Registration.Metadata,
		ClusterName: r.cfg.Registration.ClusterName,
		GroupName:   r.cfg.Registration.GroupName,
		Ephemeral:   r.cfg.Registration.Ephemeral,
	}
	return param
}

func (r *resolver) newSelectInstances(ctx context.Context, req nr.ResolveRequest) *vo.SelectInstancesParam {
	param := &vo.SelectInstancesParam{}
	param.Clusters = r.cfg.Selected.Clusters
	param.ServiceName = req.ID
	param.GroupName = r.cfg.Selected.GroupName
	param.HealthyOnly = r.cfg.Selected.HealthyOnly
	return param
}
