<?php

use Illuminate\Database\Capsule\Manager as Capsule;

require_once __DIR__ . '/../vendor/autoload.php';

$connection = [
    'driver'   => 'pgsql',
    'host'     => 'postgres',
    'port'     => 5432,
    'database' => 'dataset',
    'username' => 'root',
    'password' => 'secret',
];

$capsule = new Capsule();
$capsule->addConnection($connection);
$capsule->setAsGlobal();
$capsule->bootEloquent();

/*$company = new Company();
var_dump($company->import());*/

/*$user = new User();
var_dump($user->import());*/

$user = new DbToCsv();
var_dump($user->export());
/*var_dump((new Encounter())->export());*/
