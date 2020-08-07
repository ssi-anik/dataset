<?php

use Faker\Factory;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Schema\Blueprint;
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
        /*unlink(__DIR__ . '/dataset-default.sqlite');
        unlink(__DIR__ . '/dataset-sqlite.sqlite');*/
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

    protected function setupDatabase () {
        $connections = [
            'default' => [
                'driver'   => 'sqlite',
                'database' => __DIR__ . '/dataset-default.sqlite',
            ],

            'sqlite' => [
                'driver'   => 'sqlite',
                'database' => __DIR__ . '/dataset-sqlite.sqlite',
            ],
        ];

        $this->capsule = new Capsule($this->container);
        foreach ( $connections as $name => $config ) {
            $this->capsule->addConnection($config, $name);
        }
        $this->capsule->setAsGlobal();
        $this->capsule->bootEloquent();
    }

    protected function rollbackDatabase () {
        $connections = [ 'default', 'sqlite' ];

        foreach ( $connections as $connection ) {
            Manager::schema($connection)->dropIfExists('companies');
            Manager::schema($connection)->dropIfExists('company');
            Manager::schema($connection)->dropIfExists('members');
            Manager::schema($connection)->dropIfExists('phones');
            Manager::schema($connection)->dropIfExists('emails');
        }
    }

    protected function migrateDatabase () {
        $this->rollbackDatabase();
        $connections = [ 'default', 'sqlite' ];

        foreach ( $connections as $connection ) {
            Manager::schema($connection)->create('companies', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('image_url');
                $table->string('slug');
            });

            Manager::schema($connection)->create('company', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('image_url');
                $table->string('slug');
            });

            Manager::schema($connection)->create('members', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->smallInteger('age');
                $table->timestamps();
            });

            Manager::schema($connection)->create('phones', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('member_id');
                $table->string('number');
            });

            Manager::schema($connection)->create('emails', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('member_id');
                $table->string('email');
            });
        }
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

    protected function bindEvents ($truthy = true) {
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
        $this->dispatcher->listen($events, $this->getListener($truthy));
    }

    protected function getFaker ($locale = 'en_US') {
        return Factory::create($locale);
    }
}