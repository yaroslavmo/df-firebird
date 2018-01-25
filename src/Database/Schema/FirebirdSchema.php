<?php

namespace DreamFactory\Core\Firebird\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;

class FirebirdSchema extends SqlSchema
{
    protected function getTableNames($schema = '')
    {
        $sql = <<<'SQL'
SELECT TRIM(RDB$RELATION_NAME) AS RDB$RELATION_NAME FROM RDB$RELATIONS 
WHERE (RDB$SYSTEM_FLAG=0 OR RDB$SYSTEM_FLAG IS NULL) and (RDB$VIEW_BLR IS NULL);
SQL;

        $rows = $this->selectColumn($sql);
        $names = [];
        foreach ($rows as $resourceName) {
            $schemaName = $schema;
            $internalName = $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    protected function getViewNames($schema = '')
    {
        $sql = <<<'SQL'
SELECT TRIM(RDB$RELATION_NAME) AS RDB$RELATION_NAME FROM RDB$RELATIONS 
WHERE (RDB$SYSTEM_FLAG=0 OR RDB$SYSTEM_FLAG IS NULL) and (RDB$RELATION_TYPE = 1);
SQL;

        $rows = $this->selectColumn($sql);
        $names = [];
        foreach ($rows as $resourceName) {
            $schemaName = $schema;
            $internalName = $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    protected function loadTableColumns(TableSchema $table)
    {
        $params = [':table1' => $table->resourceName];

        // get all triggers for this table to check for auto incrementation
        $sql = <<<'SQL'
SELECT RDB$TRIGGER_SOURCE 
FROM RDB$TRIGGERS
WHERE RDB$RELATION_NAME = :table1 AND RDB$TRIGGER_TYPE = 1 and RDB$TRIGGER_INACTIVE = 0
SQL;
        $triggers = $this->selectColumn($sql, $params);

        $sql = <<<'SQL'
            SELECT TRIM(r.RDB$FIELD_NAME) AS "name", 
            f.RDB$FIELD_TYPE AS "type", 
            f.RDB$FIELD_SUB_TYPE AS "sub_type", 
            f.RDB$FIELD_LENGTH AS "length", 
            f.RDB$CHARACTER_LENGTH AS "char_length", 
            f.RDB$FIELD_PRECISION AS "precision", 
            f.RDB$FIELD_SCALE AS "scale", 
            r.RDB$NULL_FLAG as "non_nullable", 
            r.RDB$DEFAULT_SOURCE AS "default", 
            r.RDB$DESCRIPTION AS "comment",
            TRIM(cs.RDB$CHARACTER_SET_NAME) as "character_set",
            TRIM(cl.RDB$COLLATION_NAME) as "collation"
            FROM RDB$RELATION_FIELDS r 
            LEFT OUTER JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME 
            LEFT OUTER JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID 
            LEFT OUTER JOIN RDB$COLLATIONS cl ON cl.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID AND cl.RDB$COLLATION_ID = f.RDB$COLLATION_ID
            WHERE r.RDB$RELATION_NAME = :table1 
            ORDER BY r.RDB$FIELD_POSITION
SQL;
        $result = $this->connection->select($sql, $params);
        foreach ($result as $column) {
            $column = array_change_key_case((array)$column, CASE_LOWER);
            $c = new ColumnSchema(['name' => $column['name']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->allowNull = !array_get_bool($column,'non_nullable');
            foreach ($triggers as $trigger) {
                if (false !== stripos($trigger, $c->name)) {
                    $c->autoIncrement = true;
                    $seq = stristr($trigger, '.nextval', true);
                    $seq = substr($seq, strrpos($seq, ' ') + 1);
                    $table->sequenceName = $seq;
                }
            }
            $type = array_get($column, 'type');
            $subType = array_get($column, 'sub_type');
            switch ((int)$type) {
                case 7:
                    $c->dbType = DbSimpleTypes::TYPE_SMALL_INT;
                    switch ((int)$subType) {
                        case 1:
                            $c->dbType = 'numeric';
                            break;
                        case 2:
                            $c->dbType = 'decimal';
                            break;
                    }
                    break;
                case 8:
                    $c->dbType = DbSimpleTypes::TYPE_INTEGER;
                    switch ((int)$subType) {
                        case 1:
                            $c->dbType = 'numeric';
                            break;
                        case 2:
                            $c->dbType = 'decimal';
                            break;
                    }
                    break;
                case 10:
                    $c->dbType = DbSimpleTypes::TYPE_FLOAT;
                    break;
                case 12:
                    $c->dbType = DbSimpleTypes::TYPE_DATE;
                    break;
                case 13:
                    $c->dbType = DbSimpleTypes::TYPE_TIME;
                    break;
                case 14:
                    $c->dbType = 'char';
                    break;
                case 16:
                    $c->dbType = DbSimpleTypes::TYPE_BIG_INT;
                    switch ((int)$subType) {
                        case 1:
                            $c->dbType = 'numeric';
                            break;
                        case 2:
                            $c->dbType = 'decimal';
                            break;
                    }
                    break;
                case 27:
                    $c->dbType = DbSimpleTypes::TYPE_DOUBLE;
                    break;
                case 35:
                    $c->dbType = DbSimpleTypes::TYPE_TIMESTAMP;
                    break;
                case 37:
                    $c->dbType = 'varchar';
                    break;
                case 261:
                    $c->dbType = 'blob';
                    if (1 === (int)$subType) {
                        $c->dbType = 'text';
                    }
                    break;
                default:
                    $c->dbType = strval($type) . ':' . strval($subType);
            }
            if (isset($column['collation']) && !empty($column['collation'])) {
                $collation = $column['collation'];
                if (0 === stripos($collation, 'utf') || 0 === stripos($collation, 'ucs')) {
                    $c->supportsMultibyte = true;
                }
            }
            if (isset($column['comment'])) {
                $c->comment = $column['comment'];
            }

            $c->precision = intval($column['precision']);
            $c->scale = intval($column['scale']);
            // all of this is for consistency across drivers
            if ($c->precision > 0) {
                if ($c->scale <= 0) {
                    $c->size = $c->precision;
                    $c->scale = null;
                }
            } else {
                $c->precision = null;
                $c->scale = null;
                $c->size = intval($column['char_length']);
                if ($c->size <= 0) {
                    $c->size = null;
                }
            }
            $this->extractLimit($c, $c->dbType);
            $c->fixedLength = $this->extractFixedLength($c->dbType);
            $this->extractType($c, $c->dbType);
            $this->extractDefault($c, $column['default']);

            $table->addColumn($c);
        }
    }

    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        $sql = <<<'SQL'
      SELECT TRIM(rc.RDB$CONSTRAINT_NAME) AS constraint_name,
      TRIM(rc.RDB$CONSTRAINT_TYPE) AS constraint_type,
      TRIM(i.RDB$RELATION_NAME) AS table_name,
      TRIM(s.RDB$FIELD_NAME) AS column_name,
      TRIM(i.RDB$DESCRIPTION) AS description,
      TRIM(refc.RDB$UPDATE_RULE) AS update_rule,
      TRIM(refc.RDB$DELETE_RULE) AS delete_rule,
      TRIM(i2.RDB$RELATION_NAME) AS referenced_table_name,
      TRIM(s2.RDB$FIELD_NAME) AS referenced_column_name,
      (s.RDB$FIELD_POSITION + 1) AS field_position
      FROM RDB$INDEX_SEGMENTS s
      LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
      LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
      LEFT JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
      LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON rc2.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
      LEFT JOIN RDB$INDICES i2 ON i2.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME
      LEFT JOIN RDB$INDEX_SEGMENTS s2 ON i2.RDB$INDEX_NAME = s2.RDB$INDEX_NAME AND s.RDB$FIELD_POSITION = s2.RDB$FIELD_POSITION
      WHERE rc.RDB$CONSTRAINT_TYPE IS NOT NULL
      ORDER BY i.RDB$RELATION_NAME, s.RDB$FIELD_NAME  
SQL;

        $results = $this->connection->select($sql);
        $constraints = [];
        $ts = '';
        foreach ($results as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $tn = strtolower($row['table_name']);
            $cn = strtolower($row['constraint_name']);
            $colName = array_get($row, 'column_name');
            $refColName = array_get($row, 'referenced_column_name');
            if (isset($constraints[$ts][$tn][$cn])) {
                $constraints[$ts][$tn][$cn]['column_name'] =
                    array_merge((array)$constraints[$ts][$tn][$cn]['column_name'], (array)$colName);

                if (isset($refColName)) {
                    $constraints[$ts][$tn][$cn]['referenced_column_name'] =
                        array_merge((array)$constraints[$ts][$tn][$cn]['referenced_column_name'], (array)$refColName);
                }
            } else {
                $constraints[$ts][$tn][$cn] = $row;
            }
        }

        return $constraints;
    }

    public function getPrimaryKeyCommands($table, $column)
    {
        // pre 3.0 versions need sequences and trigger to accomplish autoincrement
        $trigTable = $this->quoteTableName($table);
        $trigField = $this->quoteColumnName($column);
        $table = str_replace('.', '_', $table);
        // sequence and trigger names maximum length is 30
        if (26 < strlen($table)) {
            $table = hash('crc32', $table);
        }
        $sequence = $this->quoteTableName(strtoupper($table) . '_SEQ');
        $trigger = $this->quoteTableName(strtoupper($table) . '_TRG');

        $extras = [];
        $extras[] = "CREATE SEQUENCE $sequence";
        $extras[] = <<<SQL
CREATE TRIGGER {$trigger} FOR {$trigTable}
BEFORE INSERT AS
BEGIN
    IF ((NEW.{$trigField} IS NULL) OR 
       (NEW.{$trigField} = 0)) THEN
    BEGIN
        NEW.{$trigField} = NEXT VALUE FOR {$sequence};
    END
END;
SQL;

        return $extras;
    }

    /** {@inheritdoc} */
    public function dropTable($table)
    {
        $result = parent::dropTable($table);

        $table = str_replace(['.', '"'], ['_', ''], $table);
        // sequence and trigger names maximum length is 30
        if (26 < strlen($table)) {
            $table = hash('crc32', $table);
        }
        $sequence = $this->quoteTableName(strtoupper($table) . '_SEQ');
        $sql = <<<SQL
DROP SEQUENCE {$sequence};
SQL;
        try {
            $this->connection->statement($sql);
        } catch (\Exception $ex) {

        }

        $trigger = $this->quoteTableName(strtoupper($table) . '_TRG');
        $params = [':trigger1' => $trigger, ':trigger2' => $trigger];
        $sql = <<<SQL
DROP TRIGGER {$trigger};
SQL;
        try {
            $this->connection->statement($sql, $params);
        } catch (\Exception $ex) {

        }

        return $result;
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

    public static function isUndiscoverableType($type)
    {
        switch ($type) {
            case DbSimpleTypes::TYPE_BOOLEAN:
                return true;
        }

        return parent::isUndiscoverableType($type);
    }

    public function makeConstraintName($prefix, $table, $column = null)
    {
        $temp = parent::makeConstraintName($prefix, $table, $column);
        // must be less than 30 characters
        if (30 < strlen($temp)) {
            $temp = substr($temp, strlen($prefix . '_'));
            $temp = $prefix . '_' . hash('crc32', $temp);
        }

        return $temp;
    }

    public function getTimestampForSet()
    {
        return $this->connection->raw('(CURRENT_TIMESTAMP)');
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

            case DbSimpleTypes::TYPE_DATETIME:
                $info['type'] = 'timestamp';
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $default = 'CURRENT_TIMESTAMP';
                    // ON UPDATE CURRENT_TIMESTAMP not supported by Firebird, use triggers
                    $info['default'] = $default;
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

            case DbSimpleTypes::TYPE_DOUBLE:
                $info['type'] = 'double precision';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $national =
                    (isset($info['supports_multibyte'])) ? filter_var($info['supports_multibyte'],
                        FILTER_VALIDATE_BOOLEAN) : false;
                if ($fixed) {
                    $info['type'] = ($national) ? 'nchar' : 'char';
                } elseif ($national) {
                    $info['type'] = 'national character varying';
                } else {
                    $info['type'] = 'varchar';
                }
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $info['type'] = 'blob sub_type binary';
                break;

            case DbSimpleTypes::TYPE_TEXT:
                $info['type'] = 'blob sub_type text';
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
                        $length = ($length > 18) ? 18 : $length;
                        $info['type_extras'] = (!empty($scale)) ? "($length,$scale)" : "($length)";
                        $info['length'] = $length;
                        $info['scale'] = $scale;
                    }
                } else {
                    $range = trim($info['type_extras'], '()');
                    if (strpos($range, ',')) {
                        $length = (int)strstr($range, ',', true);
                        $scale = (int)trim(strstr($range, ','), ',');
                        if ($length > 18) {
                            $info['type_extras'] = '(18,' . $scale . ')';
                        }
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;

            case 'blob':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                    $info['length'] = $length;
                }
                break;

            case 'char':
            case 'character':
            case 'varchar':
            case 'character varying':
            case 'nchar':
            case 'national character':
            case 'national character varying':
            case 'national char':
            case 'national char varying':
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

            case 'date':
            case 'time':
            case 'timestamp':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if ('0000-00-00 00:00:00' == $default) {
                    // read back from MySQL has formatted zeros, can't send that back
                    $info['default'] = 0;
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

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? '' : ' NOT NULL';

//        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
//        if ($auto) {
//            $definition .= ' AUTO_INCREMENT';
//        }

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' UNIQUE';
        }

        return $definition;
    }
}