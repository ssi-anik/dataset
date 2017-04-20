<?php

use Dataset\Connection;
use Dataset\Dataset;

class Category extends Dataset
{
    public function __construct(Connection $database)
    {
        parent::__construct($database);
        $this->mapper = [
            "categoryID"   => 'id',
            "categoryName" => 'name',
            'description',
        ];
        #$this->headerAsTableField = true;
        $this->ignoreCsvColumns = ['picture'];
        $this->additionalFields = [
            'created_at' => date('Y-m-d h:i:s', strtotime('now')),
            'updated_at' => date('Y-m-d h:i:s', strtotime('now')),
        ];
    }

}