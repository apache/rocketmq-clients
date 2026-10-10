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

use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../ClientTrait.php';
require_once __DIR__ . '/../autoload.php';

use Apache\Rocketmq\V2\AddressScheme;

/**
 * Concrete class that uses ClientTrait to test parseEndpoints().
 */
class EndpointsTestClient
{
    use \Apache\Rocketmq\ClientTrait;

    protected function getCredentials(): ?\Apache\Rocketmq\SessionCredentials
    {
        return null;
    }

    protected function getClientIdValue(): string
    {
        return 'test-client-id';
    }

    protected function getNamespaceValue(): string
    {
        return '';
    }

    public function testParseEndpoints(string $endpoints): \Apache\Rocketmq\V2\Endpoints
    {
        return $this->parseEndpoints($endpoints);
    }
}

/**
 * Tests for parseEndpoints() in ClientTrait.
 * Mirrors Java's EndpointsTest.
 */
class EndpointsTest extends TestCase
{
    private $client;

    public function setUp(): void
    {
        $this->client = new EndpointsTestClient();
    }

    public function testEndpointsWithSingleIpv4AndPort()
    {
        $endpoints = $this->client->testParseEndpoints('127.0.0.1:8080');

        $this->assertEquals(AddressScheme::IPv4, $endpoints->getScheme(), "Scheme should be IPv4");
        $this->assertEquals(1, count($endpoints->getAddresses()), "Should have 1 address");

        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('127.0.0.1', $address->getHost(), "Host should be 127.0.0.1");
        $this->assertEquals(8080, $address->getPort(), "Port should be 8080");
    }

    public function testEndpointsWithSingleIpv4NoPort()
    {
        $endpoints = $this->client->testParseEndpoints('127.0.0.1');

        $this->assertEquals(AddressScheme::IPv4, $endpoints->getScheme(), "Scheme should be IPv4");
        $this->assertEquals(1, count($endpoints->getAddresses()), "Should have 1 address");

        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('127.0.0.1', $address->getHost(), "Host should be 127.0.0.1");
        $this->assertEquals(80, $address->getPort(), "Port should default to 80");
    }

    public function testEndpointsWithDomainAndPort()
    {
        $endpoints = $this->client->testParseEndpoints('rocketmq.apache.org:8081');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $this->assertEquals(1, count($endpoints->getAddresses()), "Should have 1 address");

        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "Host should be rocketmq.apache.org");
        $this->assertEquals(8081, $address->getPort(), "Port should be 8081");
    }

    public function testEndpointsWithDomainNoPort()
    {
        $endpoints = $this->client->testParseEndpoints('rocketmq.apache.org');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $this->assertEquals(1, count($endpoints->getAddresses()), "Should have 1 address");

        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "Host should be rocketmq.apache.org");
        $this->assertEquals(80, $address->getPort(), "Port should default to 80");
    }

    public function testEndpointsWithDomainAndHttpPrefix()
    {
        $endpoints = $this->client->testParseEndpoints('http://rocketmq.apache.org');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "HTTP prefix should be stripped");
        $this->assertEquals(80, $address->getPort(), "Port should default to 80");
    }

    public function testEndpointsWithDomainAndHttpsPrefix()
    {
        $endpoints = $this->client->testParseEndpoints('https://rocketmq.apache.org');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "HTTPS prefix should be stripped");
        $this->assertEquals(80, $address->getPort(), "Port should default to 80");
    }

    public function testEndpointsWithDomainPortAndHttpPrefix()
    {
        $endpoints = $this->client->testParseEndpoints('http://rocketmq.apache.org:8081');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "HTTP prefix should be stripped");
        $this->assertEquals(8081, $address->getPort(), "Port should be 8081");
    }

    public function testEndpointsWithDomainPortAndHttpsPrefix()
    {
        $endpoints = $this->client->testParseEndpoints('https://rocketmq.apache.org:8081');

        $this->assertEquals(AddressScheme::DOMAIN_NAME, $endpoints->getScheme(), "Scheme should be DOMAIN_NAME");
        $address = $endpoints->getAddresses()[0];
        $this->assertEquals('rocketmq.apache.org', $address->getHost(), "HTTPS prefix should be stripped");
        $this->assertEquals(8081, $address->getPort(), "Port should be 8081");
    }
}

