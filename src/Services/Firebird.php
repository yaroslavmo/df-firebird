<?php

namespace DreamFactory\Core\Firebird\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\Firebird\Resources\Table;

class Firebird extends SqlDb
{

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'firebird';
        parent::adaptConfig($config);
    }

    /**
     * {@inheritdoc}
     */
    public function getResources($only_handlers = false)
    {
        $resources = [
            Schema::RESOURCE_NAME => [
                'name'       => Schema::RESOURCE_NAME,
                'class_name' => Schema::class,
                'label'      => 'Schema',
            ],
            Table::RESOURCE_NAME  => [
                'name'       => Table::RESOURCE_NAME,
                'class_name' => Table::class,
                'label'      => 'Tables',
            ]
        ];

        return ($only_handlers) ? $resources : array_values($resources);
    }
}