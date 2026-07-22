// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/sidecar"
)

var (
	version       = "unknown.version"
	gitHash       = "unknown.gitHash"
	buildTime     = "unknown.buildTime"
	gitTag        = "unknown.tag"
	gitCommitter  = "unknown.committer"
	gitCommitTime = "unknown.commitTime"
	buildId       = "unknown.buildId"
	buildNo       = "unknown.buildNo"
	release       = "unknown.release"
)

func main() {
	path := flag.String("config", "./sidecar.yaml", "configuration filepath")
	v := flag.Bool("version", false, "display version")
	flag.Bool("v", false, "display version")
	printVersionAndExitIfRequested()
	flag.Parse()

	if *v {
		fmt.Println(version)
		return
	}

	config, err := loadConfig(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("load config: %+v\n", config)

	app, err := sidecar.New(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create sidecar failed: %v\n", err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := app.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "run sidecar failed: %v\n", err)
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	app.Stop()
	wg.Wait()
}

func printVersionAndExitIfRequested() {
	for _, arg := range os.Args[1:] {
		if arg == "-vv" {
			printDetailVersion()
			os.Exit(0)
		}
		if arg == "-v" || arg == "--version" || arg == "-version" {
			fmt.Println(version)
			os.Exit(0)
		}
	}
}

func printDetailVersion() {
	fmt.Println("Version:", version)
	fmt.Println("CommitId:", gitHash)
	fmt.Println("Tag:", gitTag)
	fmt.Println("Committer:", gitCommitter)
	fmt.Println("CommitTime:", gitCommitTime)
	fmt.Println("BuildTime:", buildTime)
	fmt.Println("BuildId:", buildId)
	fmt.Println("BuildNo:", buildNo)
	fmt.Println("Release:", release)
}

func loadConfig(path string) (*sidecar.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var config sidecar.Config
	if err := yaml.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
