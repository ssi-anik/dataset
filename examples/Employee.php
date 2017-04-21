<?php

use Dataset\Connection;
use Dataset\Dataset;

class Employee extends Dataset
{
    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->headerAsTableField = true;
        $this->additionalFields = [
            'created_at' => date('Y-m-d h:i:s', strtotime('now')),
            'updated_at' => date('Y-m-d h:i:s', strtotime('now')),
        ];
        $this->mapper = [
            'reportsTo' => [
                'reportsTo',
                function ($row) {
                    return "null" == strtolower($row['reportsTo']) ? null : $row['reportsTo'];
                },
            ],
            'region'    => [
                'region',
                function ($row) {
                    return "null" == strtolower($row['region']) ? null : $row['region'];
                },
            ],
        ];
        $this->ignoreCsvColumns = ['photo'];
    }

}