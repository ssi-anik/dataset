<?php namespace Dataset;

use PDO;

require_once 'config.php';

class Database
{
    private static $instance = null;
    private $pdo = null;

    private function __construct()
    {
        $this->pdo = 'pdo';
    }

    public static function getConnection()
    {
        if (null === static::$instance) {
            return static::$instance = (new self());
        }
        return self::$instance;
    }

    public function &getPDO()
    {
        return $this->pdo;
    }

    public function __destruct()
    {
        $this->pdo = null;
    }
}