/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package golang

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apache/rocketmq-clients/golang/pkg/utils"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	CLIENT_LOG_ROOT     = "rocketmq.client.logRoot"
	CLIENT_LOG_MAXINDEX = "rocketmq.client.logFileMaxIndex"
	CLIENT_LOG_FILESIZE = "rocketmq.client.logFileMaxSize"
	CLIENT_LOG_LEVEL    = "rocketmq.client.logLevel"
	// CLIENT_LOG_ADDITIVE        = "rocketmq.client.log.additive"
	CLIENT_LOG_FILENAME = "rocketmq.client.logFileName"
	// CLIENT_LOG_ASYNC_QUEUESIZE = "rocketmq.client.logAsyncQueueSize"
	ENABLE_CONSOLE_APPENDER = "mq.consoleAppender.enabled"
)

var sugarBaseLogger *zap.SugaredLogger

func ResetLogger() {
	InitLogger()
}

func InitLogger() {
	writeSyncer := getLogWriter()
	isStdOut := utils.GetenvWithDef(ENABLE_CONSOLE_APPENDER, "false")
	if isStdOut == "true" {
		writeSyncer = os.Stdout
	}
	encoder := getEncoder()

	var atomicLevel = zap.NewAtomicLevel()
	switch strings.ToLower(os.Getenv(CLIENT_LOG_LEVEL)) {
	case "debug":
		atomicLevel.SetLevel(zap.DebugLevel)
	case "warn":
		atomicLevel.SetLevel(zap.WarnLevel)
	case "error":
		atomicLevel.SetLevel(zap.ErrorLevel)
	default:
		atomicLevel.SetLevel(zap.InfoLevel)
	}

	core := zapcore.NewCore(encoder, writeSyncer, atomicLevel)

	logger := zap.New(core, zap.AddCaller())
	sugarBaseLogger = logger.Sugar()
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter() zapcore.WriteSyncer {
	clientLogRoot := utils.GetenvWithDef(CLIENT_LOG_ROOT, os.Getenv("user.home")+"/logs/rocketmqlogs")
	clientLogMaxIndex := utils.GetenvWithDef(CLIENT_LOG_MAXINDEX, "10")
	clientLogFileName := utils.GetenvWithDef(CLIENT_LOG_FILENAME, "rocketmq_client_go.log")
	clientLogMaxFileSize := utils.GetenvWithDef(CLIENT_LOG_FILESIZE, "1073741824")

	logFileName := clientLogRoot + "/" + clientLogFileName
	maxFileIndex, err := strconv.Atoi(clientLogMaxIndex)
	if err != nil {
		log.Printf("%s='%s' is invalid and has been replaced with the default value %s", CLIENT_LOG_MAXINDEX, clientLogMaxIndex, "10")
		maxFileIndex = 10
	}
	maxFileSize, err := strconv.Atoi(clientLogMaxFileSize)
	if err != nil {
		log.Printf("%s='%s' is invalid and has been replaced with the default value %s", CLIENT_LOG_FILESIZE, clientLogMaxFileSize, "1073741824")
		maxFileSize = 1073741824
	}
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logFileName,
		MaxSize:    maxFileSize,
		MaxBackups: maxFileIndex,
		Compress:   false,
	}
	return zapcore.AddSync(lumberJackLogger)
}

func init() {
	InitLogger()
}
