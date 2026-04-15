<?php
/**
 * Test script for interceptor framework
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\Attribute;
use Apache\Rocketmq\AttributeKey;
use Apache\Rocketmq\MessageInterceptorContextImpl;
use Apache\Rocketmq\MessageHookPoints;
use Apache\Rocketmq\MessageHookPointsStatus;
use Apache\Rocketmq\CompositedMessageInterceptor;
use Apache\Rocketmq\LoggingMessageInterceptor;

echo "=== Interceptor Framework Tests ===\n\n";

$testsPassed = 0;
$testsFailed = 0;

// Test 1: Attribute and AttributeKey
echo "Test 1: Attribute and AttributeKey\n";
try {
    $key = AttributeKey::create('test_key');
    $attr = Attribute::create('test_value');
    
    if ($key->getName() === 'test_key') {
        echo "✓ PASS: AttributeKey created with correct name\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: AttributeKey has wrong name\n";
        $testsFailed++;
    }
    
    if ($attr->get() === 'test_value') {
        echo "✓ PASS: Attribute stores and retrieves value correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Attribute value mismatch\n";
        $testsFailed++;
    }
    
    // Test attribute update
    $attr->set('new_value');
    if ($attr->get() === 'new_value') {
        echo "✓ PASS: Attribute value updated successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Attribute value not updated\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 2: MessageInterceptorContext
echo "Test 2: MessageInterceptorContext\n";
try {
    $context = new MessageInterceptorContextImpl(
        MessageHookPoints::SEND_BEFORE,
        MessageHookPointsStatus::OK
    );
    
    if ($context->getMessageHookPoints() === MessageHookPoints::SEND_BEFORE) {
        echo "✓ PASS: Context stores hook points correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Hook points mismatch\n";
        $testsFailed++;
    }
    
    if ($context->getStatus() === MessageHookPointsStatus::OK) {
        echo "✓ PASS: Context stores status correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Status mismatch\n";
        $testsFailed++;
    }
    
    // Test attributes
    $key = AttributeKey::create('test_attr');
    $attr = Attribute::create('test_value');
    $context->putAttribute($key, $attr);
    
    $retrievedAttr = $context->getAttribute($key);
    if ($retrievedAttr !== null && $retrievedAttr->get() === 'test_value') {
        echo "✓ PASS: Attributes stored and retrieved correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Attribute retrieval failed\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 3: CompositedMessageInterceptor
echo "Test 3: CompositedMessageInterceptor\n";
try {
    $composited = new CompositedMessageInterceptor();
    
    if ($composited->getInterceptorCount() === 0) {
        echo "✓ PASS: Initial interceptor count is 0\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Initial interceptor count is not 0\n";
        $testsFailed++;
    }
    
    // Add interceptor
    $loggingInterceptor = new LoggingMessageInterceptor();
    $composited->addInterceptor($loggingInterceptor);
    
    if ($composited->getInterceptorCount() === 1) {
        echo "✓ PASS: Interceptor added successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Interceptor count after add is wrong\n";
        $testsFailed++;
    }
    
    // Remove interceptor
    $removed = $composited->removeInterceptor($loggingInterceptor);
    if ($removed && $composited->getInterceptorCount() === 0) {
        echo "✓ PASS: Interceptor removed successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Interceptor removal failed\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 4: Interceptor Chain Execution Order
echo "Test 4: Interceptor Chain Execution Order\n";
try {
    $executionOrder = [];
    
    // Create test interceptors that record execution order
    $interceptor1 = new class($executionOrder) implements \Apache\Rocketmq\MessageInterceptor {
        private $order;
        public function __construct(&$order) { $this->order = &$order; }
        public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $this->order[] = 'interceptor1_before';
        }
        public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $this->order[] = 'interceptor1_after';
        }
    };
    
    $interceptor2 = new class($executionOrder) implements \Apache\Rocketmq\MessageInterceptor {
        private $order;
        public function __construct(&$order) { $this->order = &$order; }
        public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $this->order[] = 'interceptor2_before';
        }
        public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $this->order[] = 'interceptor2_after';
        }
    };
    
    $composited = new CompositedMessageInterceptor();
    $composited->addInterceptor($interceptor1);
    $composited->addInterceptor($interceptor2);
    
    $context = new MessageInterceptorContextImpl(
        MessageHookPoints::SEND_BEFORE,
        MessageHookPointsStatus::OK
    );
    
    // Execute before
    $composited->doBefore($context, []);
    
    // Execute after
    $composited->doAfter($context, []);
    
    $expectedOrder = [
        'interceptor1_before',
        'interceptor2_before',
        'interceptor2_after',  // Reverse order
        'interceptor1_after'   // Reverse order
    ];
    
    if ($executionOrder === $expectedOrder) {
        echo "✓ PASS: Interceptors executed in correct order (before: forward, after: reverse)\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Execution order incorrect\n";
        echo "  Expected: " . implode(', ', $expectedOrder) . "\n";
        echo "  Got: " . implode(', ', $executionOrder) . "\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 5: Attribute Propagation Between Interceptors
echo "Test 5: Attribute Propagation Between Interceptors\n";
try {
    $sharedKey = AttributeKey::create('shared_data');
    
    $interceptor1 = new class($sharedKey) implements \Apache\Rocketmq\MessageInterceptor {
        private $key;
        public function __construct($key) { $this->key = $key; }
        public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $context->putAttribute($this->key, \Apache\Rocketmq\Attribute::create('data_from_interceptor1'));
        }
        public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {}
    };
    
    $interceptor2 = new class($sharedKey) implements \Apache\Rocketmq\MessageInterceptor {
        private $key;
        public $receivedData = null;
        public function __construct($key) { $this->key = $key; }
        public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
            $attr = $context->getAttribute($this->key);
            $this->receivedData = $attr ? $attr->get() : null;
        }
        public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {}
    };
    
    $composited = new CompositedMessageInterceptor();
    $composited->addInterceptor($interceptor1);
    $composited->addInterceptor($interceptor2);
    
    $context = new MessageInterceptorContextImpl(
        MessageHookPoints::SEND_BEFORE,
        MessageHookPointsStatus::OK
    );
    
    $composited->doBefore($context, []);
    
    if ($interceptor2->receivedData === 'data_from_interceptor1') {
        echo "✓ PASS: Attributes propagated correctly between interceptors\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Attribute propagation failed\n";
        echo "  Expected: data_from_interceptor1\n";
        echo "  Got: " . var_export($interceptor2->receivedData, true) . "\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Summary
echo "=== Test Summary ===\n";
echo "Total Tests: " . ($testsPassed + $testsFailed) . "\n";
echo "Passed: {$testsPassed}\n";
echo "Failed: {$testsFailed}\n";

if ($testsFailed === 0) {
    echo "\n✓ All tests passed!\n";
    exit(0);
} else {
    echo "\n✗ Some tests failed\n";
    exit(1);
}
