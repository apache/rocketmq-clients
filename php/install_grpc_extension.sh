#!/bin/bash
# Quick script to check and install gRPC PHP extension

echo "=========================================="
echo "  gRPC PHP Extension Installation Helper"
echo "=========================================="
echo ""

# Check if gRPC extension is already installed
if php -m | grep -q "^grpc$"; then
    echo "✓ gRPC extension is already installed"
    php -r "echo class_exists('Grpc\\\\ChannelCredentials') ? '✓ Grpc\\ChannelCredentials class is available' : '✗ Class not found';" 
    echo ""
    exit 0
fi

echo "✗ gRPC extension is NOT installed"
echo ""
echo "PHP Version: $(php -v | head -1)"
echo "Extension Directory: $(php -r 'echo ini_get(\"extension_dir\");')"
echo ""

# Try to install via PECL
echo "Attempting to install via PECL..."
pecl channel-update pecl.php.net 2>/dev/null

if pecl install grpc 2>&1 | grep -q "Build process completed successfully"; then
    echo "✓ gRPC extension installed successfully via PECL"
    
    # Add to php.ini if not already present
    if ! grep -q "extension=grpc.so" "$(php -i | grep 'Loaded Configuration File' | sed 's/.*=> //')"; then
        echo "extension=grpc.so" >> "$(php -i | grep 'Loaded Configuration File' | sed 's/.*=> //')"
        echo "✓ Added extension=grpc.so to php.ini"
    fi
    
    echo ""
    echo "Please restart your web server or PHP-FPM if applicable"
    echo "Then run: php test_message.php produce"
    exit 0
fi

echo "✗ PECL installation failed"
echo ""
echo "Manual installation required. Please follow these steps:"
echo ""
echo "1. Download gRPC source:"
echo "   cd /tmp"
echo "   curl -L -o grpc.tgz https://pecl.php.net/get/grpc-1.80.0.tgz"
echo "   tar -xzf grpc.tgz"
echo "   cd grpc-1.80.0"
echo ""
echo "2. Build the extension:"
echo "   phpize"
echo "   ./configure"
echo "   make"
echo ""
echo "3. Install:"
echo "   sudo make install"
echo ""
echo "4. Enable in php.ini:"
echo "   echo 'extension=grpc.so' >> \$(php -i | grep 'Loaded Configuration File' | sed 's/.*=> //')"
echo ""
echo "5. Verify:"
echo "   php -m | grep grpc"
echo ""
echo "For detailed instructions, see: INSTALL_GRPC.md"
