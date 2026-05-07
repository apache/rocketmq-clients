<?php
/**
 * Test gRPC code generation with PHP language constant
 */

require_once __DIR__ . '/vendor/autoload.php';

use Apache\Rocketmq\V2\Language;

echo "=== Test gRPC Code Generation ===\n\n";

// Check if PHP constant exists
if (defined('Apache\Rocketmq\V2\Language::PHP')) {
    echo "✓ Language::PHP constant exists: " . Language::PHP . "\n";
} else {
    echo "✗ Language::PHP constant NOT found\n";
    exit(1);
}

// List all language constants
echo "\nAvailable Language constants:\n";
$reflection = new ReflectionClass(Language::class);
$constants = $reflection->getConstants();
foreach ($constants as $name => $value) {
    echo "  - {$name}: {$value}\n";
}

echo "\n✓ Test passed!\n";
