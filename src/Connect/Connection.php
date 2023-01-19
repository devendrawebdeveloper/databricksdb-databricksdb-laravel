<?php

namespace DataBricks\Laravel\Connect;

use Illuminate\Database\Connection;
use DataBricks\Laravel\Query;
use DataBricks\Laravel\QueryGrammar;
use DataBricks\Laravel\Schema;
use DataBricks\Laravel\SchemaBuilder;
use DataBricks\Laravel\SchemaGrammar;

class DataBricksConnection extends Connection
{
    /**
     * Get a schema builder instance for the connection.
     *
     * @return SchemaBuilder
     */
    public function getSchemaBuilder()
    {
        if (null === $this->schemaGrammar) {
            $this->useDefaultSchemaGrammar();
        }

        return new Schema\Builder($this);
    }

    /**
     * Get the default query grammar instance.
     *
     * @return QueryGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new Query\Grammar);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new Schema\Grammar);
    }

    /**
     * Get a new query builder instance.
     */
    public function query()
    {
        return new Query\Builder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor()
        );
    }
}
