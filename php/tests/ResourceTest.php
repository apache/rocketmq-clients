<?php
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

namespace Apache\Rocketmq\Test;

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\V2\Resource;

class ResourceTest
{
    public function testConstructorWithNameOnly()
    {
        $resource = new Resource();
        $resource->setName('foobar');
        TestRunner::assertEquals('foobar', $resource->getName(), "Name should be 'foobar'");
        TestRunner::assertEquals('', $resource->getResourceNamespace(), "Namespace should be empty by default");
    }

    public function testConstructorWithNameAndNamespace()
    {
        $resource = new Resource();
        $resource->setResourceNamespace('foo');
        $resource->setName('bar');
        TestRunner::assertEquals('bar', $resource->getName(), "Name should be 'bar'");
        TestRunner::assertEquals('foo', $resource->getResourceNamespace(), "Namespace should be 'foo'");
    }

    public function testToProtobuf()
    {
        $resource = new Resource();
        $resource->setResourceNamespace('foo');
        $resource->setName('bar');

        TestRunner::assertEquals('foo', $resource->getResourceNamespace(), "Protobuf namespace should be 'foo'");
        TestRunner::assertEquals('bar', $resource->getName(), "Protobuf name should be 'bar'");
    }

    public function testEquals()
    {
        $resource0 = new Resource();
        $resource0->setResourceNamespace('foo');
        $resource0->setName('bar');

        $resource1 = new Resource();
        $resource1->setResourceNamespace('foo');
        $resource1->setName('bar');

        TestRunner::assertEquals(
            $resource0->serializeToString(),
            $resource1->serializeToString(),
            "Same name and namespace should serialize to same value"
        );

        $resource2 = new Resource();
        $resource2->setResourceNamespace('foo0');
        $resource2->setName('bar');

        TestRunner::assertNotEquals(
            $resource0->serializeToString(),
            $resource2->serializeToString(),
            "Different namespace should serialize to different value"
        );
    }

    public function testSetterReturnsThis()
    {
        $resource = new Resource();
        $result = $resource->setName('test');
        TestRunner::assertTrue($result === $resource, "setName should return \$this for chaining");

        $result = $resource->setResourceNamespace('ns');
        TestRunner::assertTrue($result === $resource, "setResourceNamespace should return \$this for chaining");
    }
}

echo "=== ResourceTest ===\n";
TestRunner::run(new ResourceTest());
