package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	Raptor struct {
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Directory string `yaml:"directory"`
		MaxConn   int    `yaml:"max_connection"`
		Auth      string `yaml:"auth"`
	} `yaml:"raptor"`
}

func LoadConfig(path string) (*Config, error) {
	var conf Config
	f, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(f, &conf)
	if err != nil {
		log.Fatalf("config file parse failed, %v", err)
		return nil, err
	}
	return &conf, nil
}
