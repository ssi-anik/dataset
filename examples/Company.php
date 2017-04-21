<?php

use Dataset\Connection;
use Dataset\Dataset;

class Company extends Dataset
{
    protected $headerAsTableField = true;

    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->additionalFields = [
            'slug'       => function ($row, $currentRowNumber) {
                return preg_replace("/\\s+/", '-', strtolower($row['name']));
            },
            'created_at' => date('Y-m-d h:i:s', strtotime('now')),
            'updated_at' => date('Y-m-d h:i:s', strtotime('now')),
        ];
        $this->mapper = [
            'image_url' => 'url',
        ];
    }

}