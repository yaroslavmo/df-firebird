<?php

namespace DreamFactory\Core\Firebird\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class FirebirdConnector extends Connector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $options = $this->getOptions($config);

        $path = $config['database'];

        $charset = $config['charset'];

        $host = $config['host'];
        if (empty($host)) {
            throw new \InvalidArgumentException("Host not given, required.");
        }

        return $this->createConnection("firebird:dbname={$host}:{$path};charset={$charset}", $config, $options);
    }
}
