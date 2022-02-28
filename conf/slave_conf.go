package conf

import (
	"errors"
	"fmt"
	"os"
)

type config struct {
	MasterURL string
	Slot      string
	Plugin    string
	LSN       string
	Password  string
	User      string
}

var (
	Conf *config
)

func Init() {
	dsn := "postgres://localhost:5432/postgres?application_name=cdc"
	plugin := "test_decoding"
	slot := "test_slot"
	Conf = new(config)

	key := os.Getenv("MASTER_URL")
	if key == "" {
		key = dsn
	}
	Conf.MasterURL = key
	key = os.Getenv("SLOT")
	if key == "" {
		key = slot
	}
	Conf.Slot = key
	key = os.Getenv("PLUGIN")
	if key == "" {
		key = plugin
	}
	Conf.Plugin = key

	key = os.Getenv("USER")
	if key == "" {
		key = "postgres"
	}
	Conf.User = key

	key = os.Getenv("PASSWORD")
	if key == "" {
		key = "123"
	}
	Conf.Password = key

}

func MustEnv(key string) (string, error) {
	key = os.Getenv("MASTER_URL")
	if key == "" {
		return "", errors.New(fmt.Sprintf("%s is requrid", key))
	}
	return key, nil
}
