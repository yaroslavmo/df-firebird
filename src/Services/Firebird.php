<?php

namespace DreamFactory\Core\Firebird\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;

class Firebird extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'firebird';
        $config['host'] = 'localhost';
        $config['database'] = '/var/lib/firebird/2.5/data/employee.fdb';
        $config['username'] = 'sysdba';
        $config['password'] = 'root';
        $config['charset'] = 'UTF8';
        parent::adaptConfig($config);
    }
}