<?php
namespace DreamFactory\Core\Firebird;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Firebird\Database\FirebirdConnection;
use DreamFactory\Core\Firebird\Database\Connectors\FirebirdConnector;
use DreamFactory\Core\Firebird\Database\Schema\FirebirdSchema;
use DreamFactory\Core\Firebird\Models\FirebirdConfig;
use DreamFactory\Core\Firebird\Services\Firebird;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        $this->app->resolving('db.schema', function (DbSchemaExtensions $db){
            $db->extend('firebird', function ($connection){
                return new FirebirdSchema($connection);
            });
        });

        $this->app->resolving('db', function (DatabaseManager $db){
            $db->extend('firebird', function ($config){
                $connector = new FirebirdConnector();
                $connection = $connector->connect($config);

                return new FirebirdConnection($connection, $config['database'], '', $config);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df){
            $df->addType(
                new ServiceType([
                    'name'            => 'firebird',
                    'label'           => 'Firebird',
                    'description'     => 'Database service supporting Firebird connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => FirebirdConfig::class,
                    'factory'         => function ($config){
                        return new Firebird($config);
                    },
                ])
            );
        });
    }
}
