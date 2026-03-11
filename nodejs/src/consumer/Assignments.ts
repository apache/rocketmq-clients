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

import { Assignment } from './Assignment';

export class Assignments {
  readonly #assignmentList: Assignment[];

  constructor(assignmentList: Assignment[]) {
    this.#assignmentList = assignmentList;
  }

  getAssignmentList(): Assignment[] {
    return this.#assignmentList;
  }

  equals(other?: Assignments): boolean {
    if (this === other) return true;
    if (!other) return false;
    if (this.#assignmentList.length !== other.#assignmentList.length) return false;
    for (let i = 0; i < this.#assignmentList.length; i++) {
      if (!this.#assignmentList[i].equals(other.#assignmentList[i])) {
        return false;
      }
    }
    return true;
  }

  toString(): string {
    return `Assignments{assignmentList=[${this.#assignmentList.map(a => a.toString()).join(', ')}]}`;
  }
}
