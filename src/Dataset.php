<?php namespace Dataset;


abstract class Dataset
{
    protected $path = '';
    protected $excludeHeader = false;
    protected $table = '';
    protected $mapper = [];
    private $database = null;

    public function __construct(Database $database)
    {
        $this->database = $database;
    }

    public function getPath()
    {
        return $this->path;
    }

    public function getMapper()
    {
        return $this->mapper;
    }

    public function import()
    {
        return $this;
    }
}