<?php

namespace DataBricks\Laravel;

use Illuminate\Support\ServiceProvider;
use DataBricks\Laravel\Connect\DataBricksConnection;
use DataBricks\Laravel\Connect\Connector;

class DataBricksProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register(): void
    {
        DataBricksConnection::resolverFor('databricks', function ($connection, $database, $prefix, $config) {
            return new DataBricksConnection($connection, $database, $prefix, $config);
        });
    }

    public function boot()
    {
        $this->app->bind('db.connector.databricks', Connector::class);
    }
}
