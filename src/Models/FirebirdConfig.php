<?php

namespace DreamFactory\Core\Firebird\Models;

use DreamFactory\Core\SqlDb\Models\SqlDbConfig;

class FirebirdConfig extends SqlDbConfig
{
    /** {@inheritdoc} */
    public static function getDriverName()
    {
        return 'firebird';
    }

    /** {@inheritdoc} */
    public static function getDefaultPort()
    {
        return 3050;
    }

    /** {@inheritdoc} */
    protected function getConnectionFields()
    {
        $fields = parent::getConnectionFields();

        return array_merge($fields, ['charset']);
    }

    /** {@inheritdoc} */
    public static function getDefaultConnectionInfo()
    {
        $defaults = parent::getDefaultConnectionInfo();
        $defaults[] = [
            'name'        => 'charset',
            'label'       => 'Character Set',
            'type'        => 'string',
            'description' => 'The character set to use for this connection, i.e. ' . static::getDefaultCharset()
        ];

        return $defaults;
    }

    /** {@inheritdoc} */
    public static function getConfigSchema()
    {
        $schema = parent::getConfigSchema();
        array_pop($schema);             // Remove statement
        array_pop($schema);             // Remove attributes
        array_pop($schema);             // Remove options
        $charset = array_pop($schema);  // Save charset
        array_pop($schema);             // Remove schema
        array_push($schema, $charset);  // Restore charset

        return $schema;
    }
}