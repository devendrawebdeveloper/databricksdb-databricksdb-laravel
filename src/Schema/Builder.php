<?php

namespace DataBricks\Laravel\Schema;

use Closure;
use Illuminate\Database\Schema\MySqlBuilder;

class Builder extends MySqlBuilder
{
    /**
     * @param  string  $table
     * @param  Closure|null  $callback
     * @return \Illuminate\Database\Schema\Blueprint
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        // Set the resolver and then call the parent method so that we don't have
        // to duplicate the prefix generation logic. We don't bind our Blueprint
        // into the container in place of the base Blueprint because we might
        // not always be using DataBricks even if the package is installed.
        $this->blueprintResolver(function ($table, $callback, $prefix) {
            return new Blueprint($table, $callback, $prefix);
        });

        return parent::createBlueprint($table, $callback);
    }

    /**
     * Drop all tables from the database.
     *
     * @return void
     */
    public function dropAllTables()
    {
        $tables = [];

        foreach ($this->getAllTables() as $row) {
            $row = (array) $row;

            $tables[] = reset($row);
        }

        if (empty($tables)) {
            return;
        }

        foreach ($tables as $table) {
            $this->connection->statement(
                $this->grammar->compileDropAllTables([$table])
            );
        }
    }
}