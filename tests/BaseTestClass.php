<?php

use Faker\Factory;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Events\Dispatcher;
use PHPUnit\Framework\TestCase;

abstract class BaseTestClass extends TestCase
{
    protected $container, $dispatcher, $capsule;

    public static function setUpBeforeClass () : void {
        parent::setUpBeforeClass();
        touch(__DIR__ . '/dataset-default.sqlite');
        touch(__DIR__ . '/dataset-sqlite.sqlite');
    }

    public static function tearDownAfterClass () : void {
        parent::tearDownAfterClass();
        unlink(__DIR__ . '/dataset-default.sqlite');
        unlink(__DIR__ . '/dataset-sqlite.sqlite');
    }

    protected function setUp () : void {
        parent::setUp();
        $this->setUpContainer();
        $this->setUpEventDispatcher();
        $this->bindInContainer('events', $this->dispatcher);
        $this->setupDatabase();
        $this->migrateDatabase();
    }

    protected function tearDown () : void {
        parent::tearDown();
        //        $this->rollbackDatabase();
    }

    protected function setUpContainer () {
        $this->container = new Container();
    }

    final protected function getDatabaseConnections () {
        return [
            'default' => [
                'driver'   => 'sqlite',
                'database' => __DIR__ . '/dataset-default.sqlite',
            ],

            'sqlite' => [
                'driver'   => 'sqlite',
                'database' => __DIR__ . '/dataset-sqlite.sqlite',
            ],
        ];
    }

    protected function setupDatabase () {
        $connections = $this->getDatabaseConnections();

        $this->capsule = new Capsule($this->container);
        foreach ( $connections as $name => $config ) {
            $this->capsule->addConnection($config, $name);
        }
        $this->capsule->setAsGlobal();
        $this->capsule->bootEloquent();
    }

    abstract protected function rollbackDatabase ();

    abstract protected function migrateDatabase ();

    protected function createTableMigration ($connection, $table, $callback) {
        Manager::schema($connection)->create($table, $callback);
    }

    protected function alterTableMigration ($connection, $table, $callback) {
        Manager::schema($connection)->table($table, $callback);
    }

    protected function rollbackMigration ($connection, $table) {
        Manager::schema($connection)->dropIfExists($table);
    }

    protected function setUpEventDispatcher () {
        $this->dispatcher = new Dispatcher($this->container);
    }

    protected function bindInContainer ($name, $concrete) {
        $this->container->bind($name, function () use ($concrete) {
            return $concrete;
        });
    }

    protected function formatEventName ($name, $type = 'reader') {
        return 'dataset.' . $type . '.' . $name;
    }

    protected function addEventListener ($event, $listener) {
        $this->dispatcher->listen((array) $event, $listener);
    }

    private function getListener ($truthy = true) {
        return $truthy
            ? function (...$payload) {
                return true;
            }
            : function (...$payload) {
                return false;
            };
    }

    protected function listenToAvailableEvents ($truthy = true) {
        $events = [
            'dataset.reader.starting',
            'dataset.reader.preparing_reader',
            'dataset.reader.exception',
            'dataset.reader.iteration.stopped',
            'dataset.reader.iteration.completed',
            'dataset.reader.iteration.batch',
            'dataset.reader.iteration.started',
            'dataset.reader.exiting',

            'dataset.writer.starting',
            'dataset.writer.preparing_writer',
            'dataset.writer.exception',
            'dataset.writer.iteration.stopped',
            'dataset.writer.iteration.completed',
            'dataset.writer.iteration.batch',
            'dataset.writer.iteration.started',
            'dataset.writer.exiting',
        ];
        $this->addEventListener($events, $this->getListener($truthy));
    }

    protected function getFaker ($locale = 'en_US') {
        return Factory::create($locale);
    }
}
