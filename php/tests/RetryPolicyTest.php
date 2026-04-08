<?php
/**
 * Retry mechanism test
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\RetryPolicy;

echo "=== Retry Mechanism Test ===\n\n";

// Test 1: Basic configuration
echo "Test 1: Create default retry policy\n";
$policy = new ExponentialBackoffRetryPolicy();
echo "最大重试次数: " . $policy->getMaxAttempts() . "\n";
echo "初始退避: " . $policy->getInitialBackoff() . "ms\n";
echo "最大退避: " . $policy->getMaxBackoff() . "ms\n";
echo "退避倍数: " . $policy->getBackoffMultiplier() . "\n";
echo "策略描述: " . $policy . "\n\n";

// Test 2: Custom configuration
echo "Test 2: Create custom retry policy\n";
$customPolicy = new ExponentialBackoffRetryPolicy(5, 200, 10000, 3.0);
echo "最大重试次数: " . $customPolicy->getMaxAttempts() . "\n";
echo "初始退避: " . $customPolicy->getInitialBackoff() . "ms\n";
echo "最大退避: " . $customPolicy->getMaxBackoff() . "ms\n";
echo "退避倍数: " . $customPolicy->getBackoffMultiplier() . "\n\n";

// Test 3: Immediate retry policy
echo "Test 3: Immediate retry policy (no delay)\n";
$immediatePolicy = ExponentialBackoffRetryPolicy::immediatelyRetryPolicy(3);
for ($i = 1; $i <= 5; $i++) {
    $delay = $immediatePolicy->getNextAttemptDelay($i);
    echo "Retry {$i} delay: {$delay}ms\n";
}
echo "\n";

// Test 4: Fixed delay policy
echo "Test 4: Fixed delay policy\n";
$fixedPolicy = ExponentialBackoffRetryPolicy::fixedDelayRetryPolicy(3, 1000);
for ($i = 1; $i <= 5; $i++) {
    $delay = $fixedPolicy->getNextAttemptDelay($i);
    echo "Retry {$i} delay: {$delay}ms\n";
}
echo "\n";

// Test 5: Exponential backoff calculation
echo "Test 5: Exponential backoff delay calculation\n";
$expPolicy = new ExponentialBackoffRetryPolicy(10, 100, 5000, 2.0);
for ($i = 1; $i <= 10; $i++) {
    $delay = $expPolicy->getNextAttemptDelay($i);
    echo "Retry {$i} delay: {$delay}ms\n";
}
echo "\n";

// Test 6: Exception retry judgment
echo "Test 6: Exception retry judgment\n";
$testExceptions = [
    ['message' => 'Connection timeout', 'code' => 4, 'expected' => true],
    ['message' => 'Service unavailable', 'code' => 14, 'expected' => true],
    ['message' => 'Too many requests', 'code' => 8, 'expected' => true],
    ['message' => 'Invalid parameter', 'code' => 3, 'expected' => false],
    ['message' => 'Permission denied', 'code' => 7, 'expected' => false],
];

foreach ($testExceptions as $test) {
    $exception = new Exception($test['message'], $test['code']);
    $shouldRetry = $policy->shouldRetry(1, $exception);
    $status = $shouldRetry === $test['expected'] ? '✓' : '✗';
    echo "{$status} Exception: \"{$test['message']}\" (code={$test['code']}), should retry: " . 
         ($test['expected'] ? 'Yes' : 'No') . ", actual: " . ($shouldRetry ? 'Yes' : 'No') . "\n";
}
echo "\n";

// Test 7: Retry count limit
echo "Test 7: Retry count limit\n";
$limitedPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000, 2.0);
$exception = new Exception("Network error", 14);
for ($i = 1; $i <= 5; $i++) {
    $canRetry = $limitedPolicy->shouldRetry($i, $exception);
    echo "Can retry after attempt {$i}: " . ($canRetry ? 'Yes' : 'No') . "\n";
}
echo "\n";

// Test 8: Parameter validation
echo "Test 8: Parameter validation\n";
try {
    $invalidPolicy = new ExponentialBackoffRetryPolicy(0);
    echo "✗ Should throw exception: maxAttempts < 1\n";
} catch (InvalidArgumentException $e) {
    echo "✓ Correctly caught exception: maxAttempts < 1\n";
}

try {
    $invalidPolicy = new ExponentialBackoffRetryPolicy(3, -1);
    echo "✗ Should throw exception: initialBackoff < 0\n";
} catch (InvalidArgumentException $e) {
    echo "✓ Correctly caught exception: initialBackoff < 0\n";
}

try {
    $invalidPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000, 0.5);
    echo "✗ Should throw exception: backoffMultiplier < 1.0\n";
} catch (InvalidArgumentException $e) {
    echo "✓ Correctly caught exception: backoffMultiplier < 1.0\n";
}
echo "\n";

echo "=== All tests completed ===\n";
