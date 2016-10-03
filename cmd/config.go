/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/go-homedir"
)

// getConfigPath get server config path.
// if the configDir is an empty string return default config path.
func getConfigPath(configDir string) (string, error) {
	if configDir != "" {
		return configDir, nil
	}
	// if the configDir is an empty string return default config path.
	homeDir, err := homedir.Dir()
	if err != nil {
		return "", err
	}
	configDir = filepath.Join(homeDir, globalMinioConfigDir)
	return configPath, nil
}

// mustGetConfigPath must get server config path.
func getDefaultConfigPath() string {
	configPath, err := getConfigPath("")
	if err != nil {
		return ""
	}
	return configPath
}

// createConfigPath create server config path.
func createConfigPath(ctx *Context) error {
	configPath := ctx.GetConfigDir()
	if err != nil {
		return err
	}
	return os.MkdirAll(configPath, 0700)
}

// isConfigFileExists - returns true if config file exists.
func isConfigFileExists(ctx *Context) bool {
	path, err := getConfigFile(ctx)
	if err != nil {
		return false
	}
	st, err := os.Stat(path)
	// If file exists and is regular return true.
	if err == nil && st.Mode().IsRegular() {
		return true
	}
	return false
}

// getConfigFile get server config file.
func getConfigFile(ctx *Context) (string, error) {
	configPath := ctx.GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configPath, globalMinioConfigFile), nil
}
