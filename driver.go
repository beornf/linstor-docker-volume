package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	linstor "github.com/LINBIT/golinstor"
	"github.com/LINBIT/golinstor/client"
	"github.com/docker/go-connections/tlsconfig"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/mitchellh/mapstructure"
	"github.com/rck/unit"
	"github.com/vrischmann/envconfig"
	"gopkg.in/ini.v1"
	"k8s.io/kubernetes/pkg/util/mount"
	mountutils "k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

const (
	datadir         = "data"
	pluginFlagKey   = "Aux/is-linstor-docker-volume"
	pluginFlagValue = "true"
	pluginFSTypeKey = "FileSystem/Type"
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
	Nodes               []string
	ReplicasOnDifferent []string
	ReplicasOnSame      []string
	DisklessStoragePool string
	DoNotPlaceWithRegex string
	FS                  string
	FSOpts              string
	MountOpts           []string
	StoragePool         string
	Size                string
	SizeKiB             uint64
	Replicas            int32
	DisklessOnRemaining bool
}

type LinstorDriver struct {
	config  string
	node    string
	root    string
	mounter *mount.SafeFormatAndMount
	resizer *mountutils.ResizeFs
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
		resizer: mountutils.NewResizeFs(exec.New()),
	}
}

const defaultHost = "localhost"

func (l *LinstorDriver) tryConnect(urls []*url.URL) (*url.URL, []error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errChan := make(chan error)
	indexChan := make(chan int)
	doneChan := make(chan bool)
	wg.Add(len(urls))
	for i := range urls {
		i := i
		go func() {
			defer wg.Done()
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", urls[i].Host)
			if err != nil {
				errChan <- err
				return
			}
			cancel()
			conn.Close()
			indexChan <- i
		}()
	}

	go func() {
		wg.Wait()
		doneChan <- true
	}()

	var errs []error
	for {
		select {
		case result := <-indexChan:
			return urls[result], nil
		case err := <-errChan:
			errs = append(errs, err)
		case <-doneChan:
			return nil, errs
		}
	}
}

func getScheme(urlSplit []string) string {
	if urlSplit[0] == "https" || urlSplit[0] == "linstor+ssl" {
		return "https"
	}
	return "http"
}

func defaultPort(scheme string) string {
	if scheme == "https" {
		return "3371"
	}
	return "3370"
}

func (l *LinstorDriver) parseBaseURL(urlString string) (*url.URL, error) {
	// Check scheme
	urlSplit := strings.Split(urlString, "://")

	if len(urlSplit) == 1 {
		if urlSplit[0] == "" {
			urlSplit[0] = defaultHost
		}
		urlSplit = []string{getScheme(urlSplit), urlSplit[0]}
	}

	if len(urlSplit) != 2 {
		return nil, fmt.Errorf("URL with multiple scheme separators. parts: %v", urlSplit)
	}
	scheme, endpoint := urlSplit[0], urlSplit[1]
	if scheme == "linstor+ssl" {
		scheme = "https"
	}

	// Check port
	endpointSplit := strings.Split(endpoint, ":")
	if len(endpointSplit) == 1 {
		endpointSplit = []string{endpointSplit[0], defaultPort(scheme)}
	}
	if len(endpointSplit) != 2 {
		return nil, fmt.Errorf("URL with multiple port separators. parts: %v", endpointSplit)
	}
	host, port := endpointSplit[0], endpointSplit[1]

	return url.Parse(fmt.Sprintf("%s://%s:%s", scheme, host, port))
}

func (l *LinstorDriver) parseURLs(urls string) ([]*url.URL, error) {
	var result []*url.URL
	for _, controller := range strings.Split(urls, ",") {
		url, err := l.parseBaseURL(controller)
		if err != nil {
			return nil, err
		}
		result = append(result, url)
	}

	return result, nil
}

func (l *LinstorDriver) findRespondingController(controllers []*url.URL) (*url.URL, error) {
	switch num := len(controllers); {
	case num > 1:
		url, errors := l.tryConnect(controllers)
		if errors != nil {
			log.Println("Unable to connect to any of the given controller hosts:")
			for _, e := range errors {
				log.Printf("   - %v", e)
			}
		}
		if len(controllers) <= len(errors) {
			return nil, fmt.Errorf("Could not connect to any controller: \n %q ", errors)
		}
		return url, nil
	case num == 1:
		return controllers[0], nil
	default:
		return nil, fmt.Errorf("No controller to connect to")
	}
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

	baseURLs, err := l.parseURLs(config.Controllers)

	if err != nil {
		return nil, err
	}

	baseURL, err := l.findRespondingController(baseURLs)

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

	if err = decoder.Decode(options); err != nil {
		return nil, err
	}

	// convert string Size to SizeKiB
	if params.Size == "" {
		params.Size = "100MB"
	}
	u := unit.MustNewUnit(unit.DefaultUnits)
	strSize := params.Size
	v, err := u.ValueFromString(strSize)
	if err != nil {
		return nil, fmt.Errorf("Could not convert '%s' to bytes: %v", strSize, err)
	}
	bytes := v.Value
	lower := 4 * unit.M
	if bytes < lower {
		bytes = lower
	}
	params.SizeKiB = uint64(bytes / unit.K)

	if params.FS == "" {
		params.FS = "ext4"
	}

	if params.Replicas == 0 {
		params.Replicas = 2
	}

	return params, nil
}

func (l *LinstorDriver) resourcesCreate(ctx context.Context, c *client.Client, req *volume.CreateRequest, params *LinstorParams) error {
	err := c.ResourceDefinitions.CreateVolumeDefinition(ctx, req.Name, client.VolumeDefinitionCreate{
		VolumeDefinition: client.VolumeDefinition{
			SizeKib: params.SizeKiB,
		},
	})
	if err != nil {
		return err
	}
	if len(params.Nodes) == 0 {
		return c.Resources.Autoplace(ctx, req.Name, client.AutoPlaceRequest{
			DisklessOnRemaining: params.DisklessOnRemaining,
			SelectFilter: client.AutoSelectFilter{
				PlaceCount:           params.Replicas,
				StoragePool:          params.StoragePool,
				NotPlaceWithRscRegex: params.DoNotPlaceWithRegex,
				ReplicasOnSame:       params.ReplicasOnSame,
				ReplicasOnDifferent:  params.ReplicasOnDifferent,
			},
		})
	}
	for _, node := range params.Nodes {
		err = c.Resources.Create(ctx, l.toDiskfullCreate(req.Name, node, params))
		if err != nil {
			return err
		}
	}
	return nil
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
			Props: map[string]string{
				pluginFlagKey:           pluginFlagValue,
				pluginFSTypeKey:         params.FS,
				"FileSystem/MkfsParams": params.FSOpts,
			},
		},
	})
	if err != nil {
		return err
	}
	err = l.resourcesCreate(ctx, c, req, params)
	if err != nil {
		resources, err := c.Resources.GetAll(ctx, req.Name)
		if err == nil && len(resources) == 0 {
			c.ResourceDefinitions.Delete(ctx, req.Name)
		}
	}
	return err
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
	if resourceDef.Props[pluginFlagKey] != pluginFlagValue {
		return nil, fmt.Errorf("Volume '%s' is not managed by this plugin", req.Name)
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
		if resourceDef.Props[pluginFlagKey] != pluginFlagValue {
			continue
		}
		vols = append(vols, &volume.Volume{
			Name:       resourceDef.Name,
			Mountpoint: l.mountPoint(resourceDef.Name),
		})
	}
	return &volume.ListResponse{Volumes: vols}, nil
}

func (l *LinstorDriver) Remove(req *volume.RemoveRequest) error {
	return l.remove(req.Name, true)
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
	// properties are not merged, so we have to query the resdef
	// as we set the property there
	resdef, err := c.ResourceDefinitions.Get(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	fstype, ok := resdef.Props[pluginFSTypeKey]
	if !ok {
		return nil, fmt.Errorf("Volume '%s' did not contain a file system key", req.Name)
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
		return nil, fmt.Errorf("Unable to get exclusive open on %s", source)
	}
	target := l.realMountPath(req.Name)
	if err = l.mounter.MakeDir(target); err != nil {
		return nil, err
	}
	err = l.mounter.Mount(source, target, fstype, params.MountOpts)
	if err != nil {
		return nil, err
	}

	mnt := l.reportedMountPath(req.Name)
	if _, err = os.Stat(mnt); os.IsNotExist(err) { // check for remount
		if err = l.mounter.MakeDir(mnt); err != nil {
			return nil, err
		}
	}

	needResize, err := l.resizer.NeedResize(source, target)
	if err != nil {
		return nil, err
	}
	if needResize {
		if _, err = l.resizer.Resize(source, target); err != nil {
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
	if err = l.mounter.Unmount(target); err != nil {
		return err
	}

	// try to remove now unused dir
	_ = os.Remove(target)

	diskless, err := l.isDiskless(req.Name)
	// in this case we don't really care about the error, just log it, and keep the diskless assignment.
	if err != nil {
		log.Println(err)
	} else if diskless {
		return l.remove(req.Name, false)
	}

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

func (l *LinstorDriver) isDiskless(name string) (bool, error) {
	lopt := client.ListOpts{Resource: []string{name}, Node: []string{l.node}}
	c, err := l.newClient()
	if err != nil {
		return false, err
	}
	ctx := context.Background()

	// view to get storage information as well
	resources, err := c.Resources.GetResourceView(ctx, &lopt)
	if err != nil {
		return false, err
	}
	if len(resources) != 1 {
		return false, errors.New("Resource filter has to contain exactly one resource")
	}
	if len(resources[0].Volumes) != 1 {
		return false, errors.New("There has to be exactly one volume in the resource")
	}

	return resources[0].Volumes[0].ProviderKind == client.DISKLESS, nil
}

func (l *LinstorDriver) remove(name string, global bool) error {
	c, err := l.newClient()
	if err != nil {
		return err
	}
	ctx := context.Background()

	if !global {
		return c.Resources.Delete(ctx, name, l.node)
	}

	// global
	snaps, err := c.Resources.GetSnapshots(ctx, name)
	if err != nil {
		return err
	}
	for _, snap := range snaps {
		err = c.Resources.DeleteSnapshot(ctx, name, snap.Name)
		if err != nil {
			return err
		}
	}
	return c.ResourceDefinitions.Delete(ctx, name)
}
