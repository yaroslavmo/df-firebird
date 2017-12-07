<?php

namespace DreamFactory\Core\Firebird\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\Firebird\Resources\Table;

class Firebird extends SqlDb
{
    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[Table::RESOURCE_NAME]['class_name'] = Table::class;

        return $handlers;
    }


    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'firebird';
        parent::adaptConfig($config);
    }
}