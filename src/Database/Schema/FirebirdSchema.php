<?php

namespace DreamFactory\Core\Firebird\Database\Schema;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Types\Type;
use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Firebird\Database\DBAL\Driver\Ibase\Firebird\Driver;
use DreamFactory\Core\Firebird\Database\DBAL\Portability\Connection;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Enums\DbResourceTypes;
use Cache;

class FirebirdSchema extends Schema
{
    /** {@inheritdoc} */
    public function quoteTableName($name)
    {
        if (strpos($name, '.') === false) {
            return $this->quoteSimpleTableName($name);
        }
        $parts = explode('.', $name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $this->quoteSimpleTableName($part);
        }

        return implode('.', $parts);
    }

    /** {@inheritdoc} */
    public function getSupportedResourceTypes()
    {
        return [
            DbResourceTypes::TYPE_SCHEMA,
            DbResourceTypes::TYPE_TABLE,
            DbResourceTypes::TYPE_TABLE_FIELD,
        ];
    }

    /** {@inheritdoc} */
    protected function findTableNames(
        /** @noinspection PhpUnusedParameterInspection */
        $schema = ''
    ){
        $sql =
            'select rdb$relation_name from rdb$relations where rdb$view_blr is null and (rdb$system_flag is null or rdb$system_flag = 0)';
        $rows = $this->connection->select($sql);
        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $row = array_values($row);
            $schemaName = $schema;
            $resourceName = trim($row[0]);
            $internalName = $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    protected function findColumns(TableSchema $table)
    {
        $doctrine = new Driver();
        $config = $this->getConnectionConfig();
        $conn = new Connection($config, $doctrine);
        $sm = $doctrine->getSchemaManager($conn);
        $tableInfo = $sm->listTableDetails($table->name);
        $primaryKeyColumns = $tableInfo->getPrimaryKeyColumns();
        $columns = $tableInfo->getColumns();
        $foreignKeys = $tableInfo->getForeignKeys();
        $fks = [];
        /** @var ForeignKeyConstraint $foreignKey */
        foreach ($foreignKeys as $foreignKey) {
            $localField = array_get($foreignKey->getLocalColumns(), 0);
            $foreignField = array_get($foreignKey->getForeignColumns(), 0);
            $foreignTable = $foreignKey->getForeignTableName();
            $onUpdate = $foreignKey->onUpdate();
            $onDelete = $foreignKey->onDelete();

            $fks[$localField] = [
                'ref_table'     => $foreignTable,
                'ref_field'     => $foreignField,
                'ref_on_update' => $onUpdate,
                'ref_on_delete' => $onDelete
            ];
        }

        $out = [];
        /** @var Column $column */
        foreach ($columns as $column) {
            $col = $column->toArray();
            $name = array_get($col, 'name');
            $col['type'] = strtolower($col['type']->__toString());
            $col['db_type'] = $col['type'];
            $col['size'] = array_get($col, 'length');
            $col['allow_null'] = !array_get($col, 'notnull', false);
            $col['auto_increment'] = array_get($col, 'autoincrement');
            $col['is_primary_key'] = in_array($name, $primaryKeyColumns);
            $col['is_foreign_key'] = in_array($name, array_keys($fks));
            $col['ref_table'] = (array_get($fks, $name)) ? $fks[$name]['ref_table'] : null;
            $col['ref_field'] = (array_get($fks, $name)) ? $fks[$name]['ref_field'] : null;
            $col['ref_on_update'] = (array_get($fks, $name)) ? $fks[$name]['ref_on_update'] : null;
            $col['ref_on_delete'] = (array_get($fks, $name)) ? $fks[$name]['ref_on_delete'] : null;
            $col['is_keyword'] = $this->isReservedKeyword($name);
            $out[] = $col;
        }

        return $out;
    }

    /**
     * Checks to see if a name is a reserved keyword.
     *
     * @param string $name
     *
     * @return bool
     */
    protected function isReservedKeyword($name)
    {
        $isKeyword = Cache::remember(
            'firebird-keyword:' . $name,
            config('df.default_cache_ttl', 300),
            function () use ($name){
                $doctrine = new Driver();
                $config = $this->getConnectionConfig();
                $conn = new Connection($config, $doctrine);
                $sm = $doctrine->getSchemaManager($conn);
                $pl = $sm->getDatabasePlatform();
                $keywordList = $pl->getReservedKeywordsList();

                return $keywordList->isKeyword($name);
            }
        );

        return $isKeyword;
    }

    /** {@inheritdoc} */
    public function createTable($table, $options)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        if (empty($columns = array_get($options, 'columns'))) {
            throw new \Exception("No valid fields exist in the received table schema.");
        }

        $doctrine = new Driver();
        $config = $this->getConnectionConfig();
        $conn = new Connection($config, $doctrine);
        $sm = $doctrine->getSchemaManager($conn);
        $schema = $sm->createSchema();
        $table = $schema->createTable($tableName);
        $primaryKeys = [];

        foreach ($columns as $name => $info) {
            $options = $this->getDoctrineColumnOptions($info);
            if (filter_var(array_get($options, 'is_primary_key'), FILTER_VALIDATE_BOOLEAN) === true) {
                $primaryKeys[] = $name;
            }

            $table->addColumn($name, $options['_type'], $options);
        }

        if (!empty($primaryKeys)) {
            $table->setPrimaryKey(['id']);
        }
        $sm->dropAndCreateTable($table);

        return true;
    }

    /**
     * Generates column options for Doctrine Column type.
     *
     * @param array $info
     *
     * @return array
     */
    protected function getDoctrineColumnOptions($info)
    {
        $info = $this->cleanFieldInfo($info);
        $type = $info['type'];
        $length = array_get($info, 'length');
        $precision = array_get($info, 'precision', $length);
        $scale = array_get($info, 'scale');
        $notnull = !array_get($info, 'allow_null', true);
        $autoincrement = filter_var(array_get($info, 'auto_increment'), FILTER_VALIDATE_BOOLEAN);
        $default = array_get($info, 'default');
        $comment = array_get($info, 'description', array_get($info, 'comment', array_get($info, 'label')));

        $options = [
            'notnull'        => $notnull,
            'autoincrement'  => $autoincrement,
            '_type'          => $type,
            'is_primary_key' => array_get($info, 'is_primary_key', false)
        ];

        if (!empty($length)) {
            $options['length'] = $length;
        }
        if (!empty($scale) && !empty($precision)) {
            $options['precision'] = $precision;
            $options['scale'] = $scale;
        }
        if (!empty($default)) {
            $options['default'] = $default;
        }
        if (!empty($comment)) {
            $options['comment'] = $comment;
        }

        return $options;
    }

    /** {@inheritdoc} */
    public function dropTable($table)
    {
        $table = trim($table, '".');
        $doctrine = new Driver();
        $config = $this->getConnectionConfig();
        $conn = new Connection($config, $doctrine);
        $sm = $doctrine->getSchemaManager($conn);
        $sequences = $sm->listSequences();
        $sm->dropTable($table);
        foreach ($sequences as $sequence) {
            $sName = $sequence->getName();
            if (strpos($sName, $table . "_") !== false) {
                $sm->dropSequence($sName);
            }
        }

        return true;
    }

    /** {@inheritdoc} */
    public function dropColumns($table, $columns)
    {
        $table = trim($table, '"');
        if (is_string($columns)) {
            $columns = (array)$columns;
        }
        foreach ($columns as $key => $val) {
            $columns[$key] = trim($val, '"');
        }
        $doctrine = new Driver();
        $config = $this->getConnectionConfig();
        $conn = new Connection($config, $doctrine);
        $sm = $doctrine->getSchemaManager($conn);
        $tableInfo = $sm->listTableDetails($table);
        $cols = $tableInfo->getColumns();

        $drops = [];
        foreach ($cols as $name => $col) {
            if (in_array($name, $columns)) {
                $drops[] = $col;
            }
        }

        $td = new TableDiff($table, [], [], $drops);
        $sm->alterTable($td);
    }

    /** {@inheritdoc} */
    public function updateTable($tableSchema, $changes)
    {
        $doctrine = new Driver();
        $config = $this->getConnectionConfig();
        $conn = new Connection($config, $doctrine);
        $sm = $doctrine->getSchemaManager($conn);

        //  Is there a name update
        if (!empty($changes['new_name'])) {
            // todo change table name, has issue with references
        }

        // update column types
        if (!empty($changes['columns']) && is_array($changes['columns'])) {
            $columns = [];
            foreach ($changes['columns'] as $name => $definition) {
                $options = $this->getDoctrineColumnOptions($definition);
                $columns[] = new Column($name, Type::getType($options['_type']), $options);
            }

            $td = new TableDiff($tableSchema->getName(), $columns);
            $sm->alterTable($td);
        }
        if (!empty($changes['alter_columns']) && is_array($changes['alter_columns'])) {
            $columnDiff = [];
            foreach ($changes['alter_columns'] as $name => $definition) {
                $options = $this->getDoctrineColumnOptions($definition);
                $columnDiff[] = new ColumnDiff(
                    $name,
                    new Column($name, Type::getType($options['_type']), $options),
                    ['type', 'notnull', 'precision', 'scale', 'default', 'comment']
                );
            }

            $td = new TableDiff($tableSchema->getName(), [], $columnDiff);
            $sm->alterTable($td);
        }
        if (!empty($changes['drop_columns']) && is_array($changes['drop_columns'])) {
            $columns = [];
            foreach ($changes['drop_columns'] as $name => $definition) {
                $options = $this->getDoctrineColumnOptions($definition);
                $columns[] = new Column($name, Type::getType($options['_type']), $options);
            }

            $td = new TableDiff($tableSchema->getName(), [], [], $columns);
            $sm->alterTable($td);
        }
    }

    /**
     * Cleans and validates column info.
     *
     * @param array $info
     *
     * @return array|mixed|null|string
     * @throws \Exception
     */
    protected function cleanFieldInfo($info)
    {
        $out = [];
        $type = '';
        if (is_string($info)) {
            $type = trim($info); // cleanup
        } elseif (is_array($info)) {
            $out = $info;
            $type = (isset($info['type'])) ? $info['type'] : null;
            if (empty($type)) {
                $type = (isset($info['db_type'])) ? $info['db_type'] : null;
                if (empty($type)) {
                    throw new \Exception("Invalid schema detected - no type or db_type element.");
                }
            }
            $type = trim($type); // cleanup
        }

        if (empty($type)) {
            throw new \Exception("Invalid schema detected - no type definition.");
        }

        //  If there are extras, then pass it on through
        if ((false !== strpos($type, ' ')) || (false !== strpos($type, '('))) {
            return $type;
        }

        $out['type'] = $type;
        $this->translateSimpleColumnTypes($out);
        $this->validateColumnSettings($out);

        return $out;
    }

    /**
     * Gets connection configs.
     *
     * @return array
     */
    private function getConnectionConfig()
    {
        return [
            'host'     => $this->connection->getConfig('host'),
            'port'     => $this->connection->getConfig('port'),
            'dbname'   => $this->connection->getConfig('database'),
            'user'     => $this->connection->getConfig('username'),
            'password' => $this->connection->getConfig('password'),
            'charset'  => $this->connection->getConfig('charset'),
        ];
    }

    /** {@inheritdoc} */
    protected function translateSimpleColumnTypes(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'integer';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'integer';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $default = 'CURRENT_TIMESTAMP';
                    if (DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE === $type) {
                        $default .= ' ON UPDATE CURRENT_TIMESTAMP';
                    }
                    $info['default'] = ['expression' => $default];
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_BOOLEAN:
                $info['type'] = 'smallint';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    // convert to bit 0 or 1, where necessary
                    $info['default'] = (int)filter_var($default, FILTER_VALIDATE_BOOLEAN);
                }
                break;

            case DbSimpleTypes::TYPE_MONEY:
                $info['type'] = 'decimal';
                $info['type_extras'] = '(19,4)';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $info['type'] = 'string';
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $info['type'] = 'binary';
                break;
        }
    }

    /** {@inheritdoc} */
    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch (strtolower($type)) {
            // some types need massaging, some need other required properties
            case 'smallint':
            case 'integer':
            case 'bigint':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = intval($default);
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'real':
            case 'float':
            case 'double':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $scale =
                            (isset($info['decimals']))
                                ? $info['decimals']
                                : ((isset($info['scale'])) ? $info['scale']
                                : null);
                        $info['type_extras'] = (!empty($scale)) ? "($length,$scale)" : "($length)";
                        $info['length'] = $length;
                        $info['scale'] = $scale;
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;

            case 'char':
            case 'blob':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                    $info['length'] = $length;
                }
                break;

            case 'string':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                    $info['length'] = $length;
                } else // requires a max length
                {
                    $info['type_extras'] = '(' . static::DEFAULT_STRING_MAX_SIZE . ')';
                    $info['length'] = static::DEFAULT_STRING_MAX_SIZE;
                }
                break;

            case 'time':
            case 'timestamp':
            case 'date':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if ('0000-00-00 00:00:00' == $default) {
                    // read back from MySQL has formatted zeros, can't send that back
                    $info['default'] = 0;
                }

                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                    $info['length'] = $length;
                }
                break;
        }
    }

    /** {@inheritdoc} */
    protected function buildColumnDefinition(array $info)
    {
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? '' : ' NOT NULL';

        $default = (isset($info['default'])) ? $info['default'] : null;
        if (isset($default)) {
            if (is_array($default)) {
                $expression = (isset($default['expression'])) ? $default['expression'] : null;
                if (null !== $expression) {
                    $definition .= ' DEFAULT ' . $expression;
                }
            } else {
                $default = $this->quoteValue($default);
                $definition .= ' DEFAULT ' . $default;
            }
        }

        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($auto) {
            $definition .= ' AUTO_INCREMENT';
        }

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' UNIQUE KEY';
        }

        return $definition;
    }
}