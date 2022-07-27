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

package utils

func Abs(n int32) int32 {
	y := n >> 31
	return (n ^ y) - y
}

func Mod(n int32, m int) int {
	return int(Abs(n)) % m
}

// func CompareEndpoints(e1 *v2.Endpoints, e2 *v2.Endpoints) bool {
// 	if e1 == e2 {
// 		return true
// 	}
// 	if e1 == nil || e2 == nil {
// 		return false
// 	}

// }
