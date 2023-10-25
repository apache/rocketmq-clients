/**
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

import { ClientDuplexStream } from '@grpc/grpc-js';
import { TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../route';
import type { BaseClient } from './BaseClient';
import { ILogger } from './Logger';

export class TelemetrySession {
  #endpoints: Endpoints;
  #baseClient: BaseClient;
  #logger: ILogger;
  #stream: ClientDuplexStream<TelemetryCommand, TelemetryCommand>;

  constructor(baseClient: BaseClient, endpoints: Endpoints, logger: ILogger) {
    this.#endpoints = endpoints;
    this.#baseClient = baseClient;
    this.#logger = logger;
    this.#renewStream(true);
  }

  release() {
    this.#logger.info('Begin to release telemetry session, endpoints=%s, clientId=%s',
      this.#endpoints, this.#baseClient.clientId);
    this.#stream.end();
    this.#stream.removeAllListeners();
  }

  write(command: TelemetryCommand) {
    this.#stream.write(command);
  }

  syncSettings() {
    const command = this.#baseClient.settingsCommand();
    this.write(command);
  }

  #renewStream(inited: boolean) {
    this.#stream = this.#baseClient.createTelemetryStream(this.#endpoints);
    this.#stream.on('data', this.#onData.bind(this));
    this.#stream.once('error', this.#onError.bind(this));
    this.#stream.once('end', this.#onEnd.bind(this));
    if (!inited) {
      this.syncSettings();
    }
  }

  #onData(command: TelemetryCommand) {
    const endpoints = this.#endpoints;
    const clientId = this.#baseClient.clientId;
    const commandCase = command.getCommandCase();
    switch (commandCase) {
      case TelemetryCommand.CommandCase.SETTINGS:
        this.#logger.info('Receive settings from remote, endpoints=%s, clientId=%s',
          endpoints, clientId);
        this.#baseClient.onSettingsCommand(endpoints, command.getSettings()!);
        break;
      case TelemetryCommand.CommandCase.RECOVER_ORPHANED_TRANSACTION_COMMAND: {
        this.#logger.info('Receive orphaned transaction recovery command from remote, endpoints=%s, clientId=%s',
          endpoints, clientId);
        this.#baseClient.onRecoverOrphanedTransactionCommand(endpoints, command.getRecoverOrphanedTransactionCommand()!);
        break;
      }
      case TelemetryCommand.CommandCase.VERIFY_MESSAGE_COMMAND: {
        this.#logger.info('Receive message verification command from remote, endpoints=%s, clientId=%s',
          endpoints, clientId);
        this.#baseClient.onVerifyMessageCommand(endpoints, command.getVerifyMessageCommand()!);
        break;
      }
      case TelemetryCommand.CommandCase.PRINT_THREAD_STACK_TRACE_COMMAND: {
        this.#logger.info('Receive thread stack print command from remote, endpoints=%s, clientId=%s',
          endpoints, clientId);
        this.#baseClient.onPrintThreadStackTraceCommand(endpoints, command.getPrintThreadStackTraceCommand()!);
        break;
      }
      default: {
        const commandObj = command.toObject();
        this.#logger.warn('Receive unrecognized command from remote, endpoints=%s, commandCase=%j, command=%j, clientId=%s',
          endpoints, commandCase, commandObj, clientId);
        // should telemetry session start fail
        this.#baseClient.onUnknownCommand(endpoints, commandObj.status!);
      }
    }
  }

  #onError(err: Error) {
    this.#logger.error('Exception raised from stream response observer, endpoints=%s, clientId=%s, error=%s',
      this.#endpoints, this.#baseClient.clientId, err);
    this.release();
    setTimeout(() => {
      this.#renewStream(false);
    }, 1000);
  }

  #onEnd() {
    this.#logger.info('Receive completion for stream response observer, endpoints=%s, clientId=%s',
      this.#endpoints, this.#baseClient.clientId);
    this.release();
    setTimeout(() => {
      this.#renewStream(false);
    }, 1000);
  }
}
