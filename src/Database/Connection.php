<?php

namespace DreamFactory\Core\Firebird\Database;

use DreamFactory\Core\Firebird\Database\Query\Grammars\Grammar;

class Connection extends \Firebird\Connection
{
    /** {@inheritdoc} */
    protected function getDefaultQueryGrammar()
    {
        return new Grammar();
    }
}