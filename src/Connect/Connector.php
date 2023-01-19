<?php

namespace DataBricks\Laravel\Connect;

use Illuminate\Database\Connectors\MySqlConnector;

class Connector extends MySqlConnector
{
    private $sock;
    private $jdbc_enc;
    private $app_enc;

    public $last_search_length = 0;

    public function connect(array $config)
    {
        $this->sock = fsockopen($config['host'], $config['port']);
        $this->jdbc_enc = $config['jdbc_enc'];
        $this->app_enc = $config['app_enc'];

        $reply = $this->exchange(array('connect', $config['service_url'], $config['username'], $config['password']));
        switch ($reply[0]) {

            case 'ok':
                return $this->sock;

            default:
                return false;
        }

        return $this->sock;
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
}
