<?php namespace Dataset;


class Dataset
{
    protected $path = '';
    protected $excludeHeader = false;
    protected $table = '';
    protected $mapper = [];

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