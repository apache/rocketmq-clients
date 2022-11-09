<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: apache/rocketmq/v2/definition.proto

namespace Apache\Rocketmq\V2;

use UnexpectedValueException;

/**
 * Protobuf type <code>apache.rocketmq.v2.TransactionResolution</code>
 */
class TransactionResolution
{
    /**
     * Generated from protobuf enum <code>TRANSACTION_RESOLUTION_UNSPECIFIED = 0;</code>
     */
    const TRANSACTION_RESOLUTION_UNSPECIFIED = 0;
    /**
     * Generated from protobuf enum <code>COMMIT = 1;</code>
     */
    const COMMIT = 1;
    /**
     * Generated from protobuf enum <code>ROLLBACK = 2;</code>
     */
    const ROLLBACK = 2;

    private static $valueToName = [
        self::TRANSACTION_RESOLUTION_UNSPECIFIED => 'TRANSACTION_RESOLUTION_UNSPECIFIED',
        self::COMMIT => 'COMMIT',
        self::ROLLBACK => 'ROLLBACK',
    ];

    public static function name($value)
    {
        if (!isset(self::$valueToName[$value])) {
            throw new UnexpectedValueException(sprintf(
                    'Enum %s has no name defined for value %s', __CLASS__, $value));
        }
        return self::$valueToName[$value];
    }


    public static function value($name)
    {
        $const = __CLASS__ . '::' . strtoupper($name);
        if (!defined($const)) {
            throw new UnexpectedValueException(sprintf(
                    'Enum %s has no value defined for name %s', __CLASS__, $name));
        }
        return constant($const);
    }
}

