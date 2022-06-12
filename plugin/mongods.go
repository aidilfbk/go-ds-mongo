package main

import (
	"context"
	"fmt"
	plugin "github.com/ipfs/go-ipfs/plugin"
	repo "github.com/ipfs/go-ipfs/repo"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/pkg/errors"
	"github.com/textileio/go-ds-mongo"
	"time"
)

var Plugins = []plugin.Plugin{
	&MongostorePlugin{},
}

type MongostorePlugin struct{}

// DatastoreType is this datastore's type name (used to identify the datastore
// in the datastore config).
var DatastoreType = "mongostore"

var _ plugin.PluginDatastore = (*MongostorePlugin)(nil)

// Name returns the plugin's name, satisfying the plugin.Plugin interface.
func (*MongostorePlugin) Name() string {
	return "ds-mongostore"
}

// Version returns the plugin's version, satisfying the plugin.Plugin interface.
func (*MongostorePlugin) Version() string {
	return "0.2.0"
}

// Init initializes plugin, satisfying the plugin.Plugin interface. Put any
// initialization logic here.
func (*MongostorePlugin) Init(env *plugin.Environment) error {
	return nil
}

// DatastoreTypeName returns the datastore's name. Every datastore
// implementation must have a unique name.
func (*MongostorePlugin) DatastoreTypeName() string {
	return DatastoreType
}

type datastoreConfig struct {
	uri        string
	dbName     string
	opTimeout  time.Duration
	txnTimeout time.Duration
	collName   string
}

// DatastoreConfigParser returns a configuration parser for Delaystore configs.
func (*MongostorePlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		config := &datastoreConfig{}

		var ok bool

		config.uri, ok = params["uri"].(string)
		if !ok {
			return nil, fmt.Errorf("mongostore: no uri specified")
		}

		config.dbName, ok = params["dbName"].(string)
		if !ok {
			return nil, fmt.Errorf("mongostore: no dbName specified")
		}

		if opTimeout, ok := params["opTimeout"].(string); ok {
			var err error
			config.opTimeout, err = time.ParseDuration(opTimeout)
			if err != nil {
				return nil, errors.Wrap(err, "mongostore: unable to parse opTimeout")
			}
		}

		if txnTimeout, ok := params["txnTimeout"].(string); ok {
			var err error
			config.txnTimeout, err = time.ParseDuration(txnTimeout)
			if err != nil {
				return nil, errors.Wrap(err, "mongostore: unable to parse txnTimeout")
			}
		}

		if collName, ok := params["collName"].(string); ok {
			config.collName = collName
		}

		return config, nil
	}
}

// DiskSpec returns this datastore's config.
func (c *datastoreConfig) DiskSpec() (spec fsrepo.DiskSpec) {
	spec = fsrepo.DiskSpec{
		"type":   DatastoreType,
		"uri":    c.uri,
		"dbName": c.dbName,
	}

	if c.collName != "" {
		spec["collName"] = c.collName
	}

	if c.opTimeout != 0 {
		spec["opTimeout"] = c.opTimeout.String()
	}

	if c.txnTimeout != 0 {
		spec["txnTimeout"] = c.txnTimeout.String()
	}

	return spec
}

// Create creates or opens the datastore.
func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	var opts []mongods.Option

	if c.collName != "" {
		opts = append(opts, mongods.WithCollName(c.collName))
	}

	if c.opTimeout != 0 {
		opts = append(opts, mongods.WithOpTimeout(c.opTimeout))
	}

	if c.txnTimeout != 0 {
		opts = append(opts, mongods.WithTxnTimeout(c.txnTimeout))
	}

	return mongods.New(context.Background(), c.uri, c.dbName, opts...)
}
