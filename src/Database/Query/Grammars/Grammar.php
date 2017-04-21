<?php

namespace DreamFactory\Core\Firebird\Database\Query\Grammars;

use Firebird\Query\Grammars\FirebirdGrammar;
use Cache;

class Grammar extends FirebirdGrammar
{
    /** {@inheritdoc} */
    protected function wrapValue($value)
    {
        if ($value !== '*') {
            if($this->isKeyword($value)) {
                return '"' . str_replace('"', '""', $value) . '"';
            }
        }

        return $value;
    }

    public static function isKeyword($name)
    {
        $key = 'firebird-keyword:' . $name;
        return Cache::get($key, false);
    }
}