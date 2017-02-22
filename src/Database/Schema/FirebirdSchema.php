<?php

namespace DreamFactory\Core\Firebird\Database\Schema;

use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Database\Schema\TableSchema;

class FirebirdSchema extends Schema
{
    protected function findTableNames(
        /** @noinspection PhpUnusedParameterInspection */
        $schema = ''
    ){
        $sql = 'select rdb$relation_name from rdb$relations where rdb$view_blr is null and (rdb$system_flag is null or rdb$system_flag = 0)';

        $rows = $this->connection->select($sql);

        $defaultSchema = $this->getNamingSchema();
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $row = array_values($row);
            $schemaName = $schema;
            $resourceName = trim($row[0]);
            $internalName = $schemaName . '.' . $resourceName;
            $name = ($addSchema) ? $internalName : $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }
}