<?php

namespace DreamFactory\Core\Firebird\Database;

use DreamFactory\Core\Firebird\Database\Query\Grammars\FirebirdGrammar;
use DreamFactory\Core\Firebird\Database\Query\Processors\FirebirdProcessor;
use DreamFactory\Core\Firebird\Database\Schema\Grammars\FirebirdGrammar as SchemaGrammar;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use PDO;

class FirebirdConnection extends Connection
{
    /**
     * The Firebird connection handler.
     *
     * @var PDO
     */
    protected $connection;

    /**
     * Create a new database connection instance.
     *
     * @param  array $config
     */
    public function __construct($pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        $this->pdo = $pdo;

        $this->config = $config;

        // First we will setup the default properties. We keep track of the DB
        // name we are connected to since it is needed when some reflective
        // type commands are run such as checking whether a table exists.
        $this->database = $database;

        $this->tablePrefix = $tablePrefix;

        $this->config = $config;

        // The connection string
        $dsn = $this->getDsn($config);

        // Create the connection
        $this->connection = $this->createConnection($dsn, $config);

        // We need to initialize a query grammar and the query post processors
        // which are both very important parts of the database abstractions
        // so we initialize these to their default values while starting.
        $this->useDefaultQueryGrammar();

        $this->useDefaultPostProcessor();
    }

    /**
     * Return the DSN string from configuration
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        // Check that the host and database are not empty
        if (!empty($config['host']) && !empty ($config['database'])) {
            return 'firebird:dbname=' . $config['host'] . ':' . $config['database'] . ';charset=' . $config['charset'];
        } else {
            trigger_error('Cannot connect to Firebird Database, no host or path supplied');
        }
    }

    /**
     * Create the Firebird Connection
     *
     * @param  string $dsn
     * @param  array  $config
     * @return PDO
     */
    public function createConnection($dsn, array $config)
    {
        //Check the username and password
        if (!empty($config['username']) && !empty($config['password'])) {
            try {
                return new PDO($dsn, $config['username'], $config['password']);
            } catch (\PDOException $e) {
                trigger_error($e->getMessage());
            }
        } else {
            trigger_error('Cannot connect to Firebird Database, no username or password supplied');
        }

        return null;
    }

    /**
     * Get the default query grammar instance
     *
     * @return Query\Grammars\FirebirdGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new FirebirdGrammar;
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processors\FirebirdProcessor
     */
    protected function getDefaultPostProcessor()
    {
        return new FirebirdProcessor;
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar());
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param  string $table
     * @return Builder
     */
    public function table($table)
    {
        $processor = $this->getPostProcessor();

        $query = new Builder($this, $this->getQueryGrammar(), $processor);

        return $query->from($table);
    }

    protected function createTransaction()
    {
        $this->getPdo()->setAttribute(\PDO::ATTR_AUTOCOMMIT, 0);
        parent::createTransaction();
    }
}