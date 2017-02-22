<?php
namespace DreamFactory\Core\Firebird;

use DreamFactory\Core\Components\ServiceDocBuilder;
use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Firebird\Database\Schema\FirebirdSchema;
use DreamFactory\Core\Firebird\Models\FirebirdConfig;
use DreamFactory\Core\Firebird\Services\Firebird;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Firebird\FirebirdServiceProvider;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    use ServiceDocBuilder;

    public function register()
    {
        $this->app->register(FirebirdServiceProvider::class);
        // Add our database extensions.
        $this->app->resolving('db.schema', function (DbSchemaExtensions $db){
            $db->extend('firebird', function ($connection){
                return new FirebirdSchema($connection);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'firebird',
                    'label'           => 'Firebird',
                    'description'     => 'Database service supporting Firebird connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => FirebirdConfig::class,
                    'default_api_doc' => function ($service) {
                        return $this->buildServiceDoc($service->id, Firebird::getApiDocInfo($service));
                    },
                    'factory'         => function ($config) {
                        return new Firebird($config);
                    },
                ])
            );
        });

        // Add our table model mapping
//        $this->app->resolving('df.system.table_model_map', function (SystemTableModelMapper $df) {
//            $df->addMapping('sql_db_config', SqlDbConfig::class);
//        });
    }

//    public function boot()
//    {
//        // add migrations
//        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
//    }
}
