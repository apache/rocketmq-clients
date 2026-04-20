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

namespace Apache\Rocketmq\Route;

use Apache\Rocketmq\V2\AddressScheme as V2AddressScheme;

/**
 * Endpoints represents network endpoints with routing information
 * 
 * References Java Endpoints implementation (216 lines)
 * 
 * Key features:
 * - Parses endpoint strings (single or multiple addresses)
 * - Auto-detects address scheme (IPv4, IPv6, DOMAIN_NAME)
 * - Manages Address objects
 * - Converts to/from Protobuf Endpoints
 */
class Endpoints
{
    /**
     * Default port number
     */
    const DEFAULT_PORT = 80;
    
    /**
     * HTTP protocol prefix
     */
    const HTTP_PREFIX = 'http://';
    
    /**
     * HTTPS protocol prefix
     */
    const HTTPS_PREFIX = 'https://';
    
    /**
     * Endpoint separator for multiple addresses
     */
    const ENDPOINT_SEPARATOR = ';';
    
    /**
     * Address separator in facade string
     */
    const ADDRESS_SEPARATOR = ',';
    
    /**
     * Host-port separator
     */
    const COLON = ':';
    
    /**
     * IPv4 host pattern
     */
    const IPV4_HOST_PATTERN = '/^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])*$/';
    
    /**
     * @var string Address scheme (IPv4, IPv6, or DOMAIN_NAME)
     */
    private $scheme;
    
    /**
     * @var string URI path for gRPC target
     * Examples:
     * - domain name: dns:rocketmq.apache.org:8080
     * - ipv4: ipv4:127.0.0.1:10911,127.0.0.2:10912
     * - ipv6: ipv6:1050:0000:0000:0000:0005:0600:300c:326b:10911
     */
    private $facade;
    
    /**
     * @var Address[] List of addresses
     */
    private $addresses;
    
    /**
     * Constructor from Protobuf Endpoints
     * 
     * @param \Apache\Rocketmq\V2\Endpoints $endpoints Protobuf endpoints
     * @throws \InvalidArgumentException If no addresses provided
     */
    public function __constructFromProtobuf(\Apache\Rocketmq\V2\Endpoints $endpoints)
    {
        $this->addresses = [];
        
        foreach ($endpoints->getAddresses() as $addressProto) {
            $this->addresses[] = new Address($addressProto->getHost(), $addressProto->getPort());
        }
        
        if (empty($this->addresses)) {
            throw new \InvalidArgumentException("No available address");
        }
        
        // Convert Protobuf scheme to internal scheme
        $protoScheme = $endpoints->getScheme();
        switch ($protoScheme) {
            case V2AddressScheme::IPv4:
                $this->scheme = AddressScheme::IPv4;
                break;
            case V2AddressScheme::IPv6:
                $this->scheme = AddressScheme::IPv6;
                break;
            case V2AddressScheme::DOMAIN_NAME:
            default:
                $this->scheme = AddressScheme::DOMAIN_NAME;
                if (count($this->addresses) > 1) {
                    throw new \InvalidArgumentException("Multiple addresses not allowed in domain schema");
                }
                break;
        }
        
        // Build facade string
        $this->facade = AddressScheme::getPrefix($this->scheme);
        foreach ($this->addresses as $address) {
            $this->facade .= $address->getAddress() . self::ADDRESS_SEPARATOR;
        }
        $this->facade = substr($this->facade, 0, -1);
    }
    
    /**
     * Constructor from endpoint string
     * 
     * @param string $endpoints Endpoint string (e.g., "127.0.0.1:8080" or "127.0.0.1:8080;127.0.0.2:8081")
     * @throws \InvalidArgumentException If endpoint format is invalid
     */
    public function __constructFromString(string $endpoints)
    {
        // Remove http:// or https:// prefix
        if (strpos($endpoints, self::HTTP_PREFIX) === 0) {
            $endpoints = substr($endpoints, strlen(self::HTTP_PREFIX));
        }
        if (strpos($endpoints, self::HTTPS_PREFIX) === 0) {
            $endpoints = substr($endpoints, strlen(self::HTTPS_PREFIX));
        }
        
        $endpoints = trim($endpoints);
        
        if (empty($endpoints)) {
            throw new \InvalidArgumentException("Endpoints must not be empty");
        }
        
        // Split by semicolon for multiple endpoints
        $addressesStr = explode(self::ENDPOINT_SEPARATOR, $endpoints);
        $this->addresses = [];
        
        if (count($addressesStr) > 1) {
            // Multiple addresses - must be IP addresses (not domain names)
            $firstAddress = $addressesStr[0];
            $lastColonPos = strrpos($firstAddress, self::COLON);
            $firstHost = substr($firstAddress, 0, $lastColonPos);
            
            // Determine scheme from first address
            $this->scheme = preg_match(self::IPV4_HOST_PATTERN, $firstHost) 
                ? AddressScheme::IPv4 
                : AddressScheme::IPv6;
            
            // Parse all addresses
            foreach ($addressesStr as $addressStr) {
                $addressStr = trim($addressStr);
                if ($addressStr === '') {
                    continue;
                }
                
                $portIndex = strrpos($addressStr, self::COLON);
                $host = substr($addressStr, 0, $portIndex);
                $port = intval(substr($addressStr, $portIndex + 1));
                
                $this->addresses[] = new Address($host, $port);
            }
            
            // Build facade
            $this->facade = AddressScheme::getPrefix($this->scheme) . str_replace(
                self::ENDPOINT_SEPARATOR, 
                self::ADDRESS_SEPARATOR, 
                $endpoints
            );
            
            return;
        }
        
        // Single address
        $addressStr = trim($addressesStr[0]);
        $lastColonPos = strrpos($addressStr, self::COLON);
        
        if ($lastColonPos !== false) {
            $host = substr($addressStr, 0, $lastColonPos);
            $port = intval(substr($addressStr, $lastColonPos + 1));
        } else {
            $host = $addressStr;
            $port = self::DEFAULT_PORT;
        }
        
        // Determine scheme
        if (preg_match(self::IPV4_HOST_PATTERN, $host)) {
            $this->scheme = AddressScheme::IPv4;
        } elseif (filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
            $this->scheme = AddressScheme::IPv6;
        } else {
            $this->scheme = AddressScheme::DOMAIN_NAME;
        }
        
        // Build facade
        $this->facade = AddressScheme::getPrefix($this->scheme) . $host . self::COLON . $port;
        
        $this->addresses[] = new Address($host, $port);
    }
    
    /**
     * Constructor from scheme and address list
     * 
     * @param string $scheme Address scheme (AddressScheme::IPv4, IPv6, or DOMAIN_NAME)
     * @param Address[] $addresses List of addresses
     * @throws \InvalidArgumentException If addresses is empty or invalid
     */
    public function __constructFromSchemeAndAddresses(string $scheme, array $addresses)
    {
        if ($scheme === AddressScheme::DOMAIN_NAME && count($addresses) > 1) {
            throw new \InvalidArgumentException("Multiple addresses not allowed in domain schema");
        }
        
        if (empty($addresses)) {
            throw new \InvalidArgumentException("No available address");
        }
        
        $this->scheme = $scheme;
        $this->addresses = $addresses;
        
        // Build facade
        $this->facade = AddressScheme::getPrefix($this->scheme);
        foreach ($this->addresses as $address) {
            $this->facade .= $address->getAddress() . self::ADDRESS_SEPARATOR;
        }
        $this->facade = substr($this->facade, 0, -1);
    }
    
    /**
     * Get address scheme
     * 
     * @return string Address scheme
     */
    public function getScheme(): string
    {
        return $this->scheme;
    }
    
    /**
     * Get addresses list
     * 
     * @return Address[] List of addresses
     */
    public function getAddresses(): array
    {
        return $this->addresses;
    }
    
    /**
     * Convert to Protobuf Endpoints
     * 
     * @return \Apache\Rocketmq\V2\Endpoints Protobuf endpoints
     */
    public function toProtobuf(): \Apache\Rocketmq\V2\Endpoints
    {
        $endpointsProto = new \Apache\Rocketmq\V2\Endpoints();
        
        // Set scheme
        switch ($this->scheme) {
            case AddressScheme::IPv4:
                $endpointsProto->setScheme(V2AddressScheme::IPv4);
                break;
            case AddressScheme::IPv6:
                $endpointsProto->setScheme(V2AddressScheme::IPv6);
                break;
            case AddressScheme::DOMAIN_NAME:
            default:
                $endpointsProto->setScheme(V2AddressScheme::DOMAIN_NAME);
                break;
        }
        
        // Set addresses
        $addressesProto = [];
        foreach ($this->addresses as $address) {
            $addressesProto[] = $address->toProtobuf();
        }
        $endpointsProto->setAddresses($addressesProto);
        
        return $endpointsProto;
    }
    
    /**
     * Get facade string
     * 
     * @return string Facade string
     */
    public function getFacade(): string
    {
        return $this->facade;
    }
    
    /**
     * Get gRPC target string
     * For domain name scheme, removes the scheme prefix
     * For IP schemes, returns full facade
     * 
     * @return string gRPC target
     */
    public function getGrpcTarget(): string
    {
        if ($this->scheme === AddressScheme::DOMAIN_NAME) {
            $prefix = AddressScheme::getPrefix($this->scheme);
            return substr($this->facade, strlen($prefix));
        }
        return $this->facade;
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->facade;
    }
    
    /**
     * Check equality
     * 
     * @param Endpoints $other Other endpoints
     * @return bool Whether equal
     */
    public function equals(Endpoints $other): bool
    {
        if ($this->scheme !== $other->scheme) {
            return false;
        }
        
        if ($this->facade !== $other->facade) {
            return false;
        }
        
        if (count($this->addresses) !== count($other->addresses)) {
            return false;
        }
        
        for ($i = 0; $i < count($this->addresses); $i++) {
            if ($this->addresses[$i]->getAddress() !== $other->addresses[$i]->getAddress()) {
                return false;
            }
        }
        
        return true;
    }
}
