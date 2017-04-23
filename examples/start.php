<?php
use Dataset\Connection;
use Dotenv\Dotenv;

require_once realpath(__DIR__ . "/../vendor/autoload.php");
require_once "Company.php";
require_once "Category.php";
require_once "Employee.php";
require_once "Product.php";

$dotEnv = new Dotenv(__DIR__);
$dotEnv->load();

$host = getenv('HOST');
$username = getenv('USERNAME');
$password = getenv('PASSWORD');
$database = getenv('DATABASE');

try {
    $connection = Connection::getConnection($host, $database, $username, $password);
    $company = new Company($connection);
    #$company->import();

    $category = new Category($connection);
    $category->import();

    $employee = new Employee($connection);
    #$employee->import();

    $product = new Product($connection);
    $product->import();


} catch (Exception $exception) {
    echo $exception->getMessage() . PHP_EOL;
    print_r(json_encode($exception->getTrace()));
}