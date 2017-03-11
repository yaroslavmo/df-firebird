<?php

namespace DreamFactory\Core\Firebird\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;

class Firebird extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'firebird';
        parent::adaptConfig($config);
    }
}