<?php

namespace DataBricks\Laravel\Connect;

use DataBricks\Laravel\Connect\JDBCConnection;
use Illuminate\Database\MySqlConnection;
use DataBricks\Laravel\Query;
use DataBricks\Laravel\QueryGrammar;
use DataBricks\Laravel\Schema;
use DataBricks\Laravel\SchemaBuilder;
use DataBricks\Laravel\SchemaGrammar;

class Connection extends MySqlConnection
{
    private $sock;
    private $jdbc_enc;
    private $app_enc;

    public $last_search_length = 0;
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

    public function select($query, $bindings = [], $useReadPdo = false)
    {
        return $this->run($query, $bindings, function ($query, $bindings) use ($useReadPdo) {
            if ($this->pretending()) {
                return [];
            }

            // For select statements, we'll simply execute the query and return an array
            // of the database result set. Each element in the array will be a single
            // row from the database table, and will either be an array or objects.
            // $statement = $this->prepared(
            //     $this->getPdoForSelect($useReadPdo)->prepare($query)
            // );

            // $this->bindValues($statement, $this->prepareBindings($bindings));

            // $statement->execute();

            // return $statement->fetchAll();
            $db = new JDBCConnection();
            $result = $db->connect($this->config['service_url'], $this->config['username'], $this->config['password']);
            if (!$result) {
                die("Failed to connect");
            }

            $cursor = $db->exec($query);
            $return_array = array();
            while ($row = $db->get_result($cursor, 'fetch_array')) {
                $return_array[] = (object) $row;
            }
            $db->free_result($cursor);
            return $return_array;
        });
    }

    public function connect(array $config)
    {
        $this->sock = fsockopen($config['host'], $config['port']);
        $this->jdbc_enc = $config['jdbc_enc'];
        $this->app_enc = $config['app_enc'];

        // $reply = $this->exchange(array('connect', $config['service_url'], $config['username'], $config['password']));
        // switch ($reply[0]) {

        //     case 'ok':
        //         return $this->sock;

        //     default:
        //         return false;
        // }
    }

    private function exchange($cmd_a)
    {

        $cmd_s = '';

        foreach ($cmd_a as $tok)
            $cmd_s .= base64_encode(iconv($this->app_enc, $this->jdbc_enc, $tok)) . ' ';

        $cmd_s = substr($cmd_s, 0, -1) . "\n";

        fwrite($this->sock, $cmd_s);

        return $this->parse_reply();
    }

    private function parse_reply()
    {

        $il = explode(' ', fgets($this->sock));
        $ol = array();

        foreach ($il as $value)
            $ol[] = iconv($this->jdbc_enc, $this->app_enc, base64_decode($value));

        return $ol;
    }

    public function exec($query)
    {

        $cmd_a = array('exec', $query);

        if (func_num_args() > 1) {

            $args = func_get_args();

            for ($i = 1; $i < func_num_args(); $i++)
                $cmd_a[] = $args[$i];
        }

        $reply = $this->exchange($cmd_a);

        switch ($reply[0]) {

            case 'ok':
                return $reply[1];

            default:
                return false;
        }
    }
}
