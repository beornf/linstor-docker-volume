package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	linstor "github.com/LINBIT/golinstor"
	"github.com/LINBIT/golinstor/client"
	"github.com/docker/go-connections/tlsconfig"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/mitchellh/mapstructure"
	"github.com/vrischmann/envconfig"
	"gopkg.in/ini.v1"
	"k8s.io/kubernetes/pkg/util/mount"
)

const (
	datadir = "data"
)

type LinstorConfig struct {
	Controllers string
	Username    string
	Password    string
	CertFile    string
	KeyFile     string
	CAFile      string
}

type LinstorParams struct {
	ClientList          []string
	NodeList            []string
	ReplicasOnDifferent []string
	ReplicasOnSame      []string
	DisklessStoragePool string
	DoNotPlaceWithRegex string
	FS                  string
	MountOpts           []string
	StoragePool         string
	SizeKiB             uint64
	PlacementCount      int32
	DisklessOnRemaining bool
}

type LinstorDriver struct {
	config  string
	node    string
	root    string
	mounter *mount.SafeFormatAndMount
}

func NewLinstorDriver(config, node, root string) *LinstorDriver {
	return &LinstorDriver{
		config: config,
		node:   node,
		root:   root,
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New("/bin/mount"),
			Exec:      mount.NewOsExec(),
		},
	}
}

func (l *LinstorDriver) newBaseURL(hosts string) (*url.URL, error) {
	scheme := "http"
	host := "localhost:3370"
	if hosts != "" {
		host = strings.Split(hosts, ",")[0]
		if s := strings.Split(host, "://"); len(s) == 2 {
			if s[0] == "linstor+ssl" || s[0] == "https" {
				scheme = "https"
			}
			host = s[1]
		}
	}

	if !strings.Contains(host, ":") {
		switch scheme {
		case "http":
			host += ":3370"
		case "https":
			host += ":3371"
		}
	}
	return url.Parse(scheme + "://" + host)
}

func (l *LinstorDriver) newClient() (*client.Client, error) {
	config := new(LinstorConfig)
	if err := l.loadConfig(config); err != nil {
		return nil, err
	}

	err := envconfig.InitWithOptions(config, envconfig.Options{
		Prefix:      "LS",
		AllOptional: true,
	})
	if err != nil {
		return nil, err
	}

	baseURL, err := l.newBaseURL(config.Controllers)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := tlsconfig.Client(tlsconfig.Options{
		CertFile:           config.CertFile,
		KeyFile:            config.KeyFile,
		CAFile:             config.CAFile,
		InsecureSkipVerify: config.CAFile == "",
		ExclusiveRootPools: true,
	})
	if err != nil {
		return nil, err
	}

	return client.NewClient(
		client.BaseURL(baseURL),
		client.BasicAuth(&client.BasicAuthCfg{
			Username: config.Username,
			Password: config.Password,
		}),
		client.HTTPClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}),
	)
}

func (l *LinstorDriver) newParams(name string, options map[string]string) (*LinstorParams, error) {
	params := new(LinstorParams)
	if err := l.loadConfig(params); err != nil {
		return nil, err
	}

	if options == nil {
		return params, nil
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           params,
		WeaklyTypedInput: true,
		DecodeHook:       mapstructure.StringToSliceHookFunc(" "),
	})
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(options)
	return params, err
}

func (l *LinstorDriver) Create(req *volume.CreateRequest) error {
	params, err := l.newParams(req.Name, req.Options)
	if err != nil {
		return err
	}
	c, err := l.newClient()
	if err != nil {
		return err
	}
	ctx := context.Background()
	err = c.ResourceDefinitions.Create(ctx, client.ResourceDefinitionCreate{
		ResourceDefinition: client.ResourceDefinition{
			Name: req.Name,
		},
	})
	if err != nil {
		return err
	}
	err = c.ResourceDefinitions.CreateVolumeDefinition(ctx, req.Name, client.VolumeDefinitionCreate{
		VolumeDefinition: client.VolumeDefinition{
			SizeKib: params.SizeKiB,
		},
	})
	if err != nil {
		return err
	}
	if len(params.NodeList)+len(params.ClientList) == 0 {
		return c.Resources.Autoplace(ctx, req.Name, client.AutoPlaceRequest{
			DisklessOnRemaining: params.DisklessOnRemaining,
			SelectFilter: client.AutoSelectFilter{
				PlaceCount:           params.PlacementCount,
				StoragePool:          params.StoragePool,
				NotPlaceWithRscRegex: params.DoNotPlaceWithRegex,
				ReplicasOnSame:       params.ReplicasOnSame,
				ReplicasOnDifferent:  params.ReplicasOnDifferent,
			},
		})
	}
	for _, node := range params.NodeList {
		err = c.Resources.Create(ctx, l.toDiskfullCreate(req.Name, node, params))
		if err != nil {
			return err
		}
	}
	for _, node := range params.ClientList {
		err = c.Resources.Create(ctx, l.toDisklessCreate(req.Name, node, params))
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LinstorDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	c, err := l.newClient()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	resourceDef, err := c.ResourceDefinitions.Get(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	vol := &volume.Volume{
		Name:       resourceDef.Name,
		Mountpoint: l.mountPoint(resourceDef.Name),
	}
	return &volume.GetResponse{vol}, nil
}

func (l *LinstorDriver) List() (*volume.ListResponse, error) {
	c, err := l.newClient()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	resourceDefs, err := c.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	vols := []*volume.Volume{}
	for _, resourceDef := range resourceDefs {
		vols = append(vols, &volume.Volume{
			Name:       resourceDef.Name,
			Mountpoint: l.mountPoint(resourceDef.Name),
		})
	}
	return &volume.ListResponse{Volumes: vols}, nil
}

func (l *LinstorDriver) Remove(req *volume.RemoveRequest) error {
	c, err := l.newClient()
	if err != nil {
		return err
	}
	ctx := context.Background()
	snaps, err := c.Resources.GetSnapshots(ctx, req.Name)
	if err != nil {
		return err
	}
	for _, snap := range snaps {
		err = c.Resources.DeleteSnapshot(ctx, req.Name, snap.Name)
		if err != nil {
			return err
		}
	}
	return c.ResourceDefinitions.Delete(ctx, req.Name)
}

func (l *LinstorDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	return &volume.PathResponse{Mountpoint: l.mountPoint(req.Name)}, nil
}

func (l *LinstorDriver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	params, err := l.newParams(req.Name, nil)
	if err != nil {
		return nil, err
	}
	c, err := l.newClient()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if _, err = c.Resources.Get(ctx, req.Name, l.node); err == client.NotFoundError {
		err = c.Resources.Create(ctx, l.toDisklessCreate(req.Name, l.node, params))
		if err != nil {
			return nil, err
		}
	}
	vol, err := c.Resources.GetVolume(ctx, req.Name, l.node, 0)
	if err != nil {
		return nil, err
	}
	source := vol.DevicePath
	inUse, err := l.mounter.DeviceOpened(source)
	if err != nil {
		return nil, err
	}
	if inUse {
		return nil, fmt.Errorf("unable to get exclusive open on %s", source)
	}
	target := l.realMountPath(req.Name)
	if err = l.mounter.MakeDir(target); err != nil {
		return nil, err
	}
	err = l.mounter.FormatAndMount(source, target, params.FS, params.MountOpts)
	if err != nil {
		return nil, err
	}

	mnt := l.reportedMountPath(req.Name)
	if _, err := os.Stat(mnt); os.IsNotExist(err) { // check for remount
		if err = l.mounter.MakeDir(mnt); err != nil {
			return nil, err
		}
	}

	return &volume.MountResponse{Mountpoint: mnt}, nil
}

func (l *LinstorDriver) Unmount(req *volume.UnmountRequest) error {
	target := l.realMountPath(req.Name)
	notMounted, err := l.mounter.IsNotMountPoint(target)
	if err != nil || notMounted {
		return err
	}
	if err := l.mounter.Unmount(target); err != nil {
		return err
	}

	// try to remove now unused dir
	_ = os.Remove(target)
	return nil
}

func (l *LinstorDriver) Capabilities() *volume.CapabilitiesResponse {
	return &volume.CapabilitiesResponse{
		Capabilities: volume.Capability{Scope: "global"},
	}
}

func (l *LinstorDriver) loadConfig(result interface{}) error {
	if _, err := os.Stat(l.config); os.IsNotExist(err) {
		return nil
	}
	file, err := ini.InsensitiveLoad(l.config)
	if err != nil {
		return err
	}
	return file.Section("global").MapTo(result)
}

func (l *LinstorDriver) realMountPath(name string) string {
	return filepath.Join(l.root, name)
}

func (l *LinstorDriver) reportedMountPath(name string) string {
	return filepath.Join(l.realMountPath(name), datadir)
}

func (l *LinstorDriver) mountPoint(name string) string {
	path := l.realMountPath(name)
	notMounted, err := l.mounter.IsNotMountPoint(path)
	if err != nil || notMounted {
		return ""
	}
	return l.reportedMountPath(name)
}

func (l *LinstorDriver) toDiskfullCreate(name, node string, params *LinstorParams) client.ResourceCreate {
	props := make(map[string]string)
	if params.StoragePool != "" {
		props[linstor.KeyStorPoolName] = params.StoragePool
	}
	return client.ResourceCreate{
		Resource: client.Resource{
			Name:     name,
			NodeName: node,
			Props:    props,
		},
	}
}

func (l *LinstorDriver) toDisklessCreate(name, node string, params *LinstorParams) client.ResourceCreate {
	props := make(map[string]string)
	if params.DisklessStoragePool != "" {
		props[linstor.KeyStorPoolName] = params.DisklessStoragePool
	}
	return client.ResourceCreate{
		Resource: client.Resource{
			Name:     name,
			NodeName: node,
			Props:    props,
			Flags:    []string{linstor.FlagDiskless},
		},
	}
}