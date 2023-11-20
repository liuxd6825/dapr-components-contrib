package configupdater

import "github.com/liuxd6825/components-contrib/configuration"

type Updater interface {
	Init(props map[string]string) error
	AddKey(items map[string]*configuration.Item) error
	UpdateKey(items map[string]*configuration.Item) error
	DeleteKey(keys []string) error
}
