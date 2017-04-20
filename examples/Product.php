<?php

use Dataset\Connection;
use Dataset\Dataset;

class Product extends Dataset
{
    protected $headerAsTableField = true;
    private $connection = null;

    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->connection = $connection;
        $this->additionalFields = [
            'created_at' => date('Y-m-d h:i:s', strtotime('now')),
            'updated_at' => date('Y-m-d h:i:s', strtotime('now')),
        ];
        $this->mapper = [
            'productID'  => 'id',
            'categoryID' => [
                'categoryID',
                function ($row) {
                    $pdo = $this->connection->getPDO();
                    $statement = $pdo->prepare("SELECT * FROM categories WHERE id = ? LIMIT 1");
                    $statement->execute([$row['categoryID']]);
                    return $statement->rowCount() ? $row['categoryID'] : null;
                },
            ],
        ];
    }

}