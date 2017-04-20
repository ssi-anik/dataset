<?php namespace Dataset;

use PDO;

class Connection
{
    private static $instance = null;
    private $host, $database, $username, $password, $driver, $attributes;
    private $pdo = null;

    private function __construct($host, $database, $username, $password, $driver)
    {
        $this->host = $host;
        $this->database = $database;
        $this->username = $username;
        $this->password = $password;
        $this->driver = $driver;
        $this->attributes = [
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_BOTH,
        ];
        $this->pdo = new PDO("{$this->driver}:host={$this->host};dbname={$this->database};", $this->username, $this->password, $this->attributes);
    }

    public static function getConnection($host, $database, $username, $password, $driver = 'mysql')
    {
        if (null === static::$instance) {
            return static::$instance = (new self($host, $database, $username, $password, $driver));
        }

        return self::$instance;
    }

    public function getPDO()
    {
        return $this->pdo;
    }

    public function __destruct()
    {
        $this->pdo = null;
    }
}