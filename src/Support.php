<?php

namespace Dataset;

use Doctrine\Inflector\Inflector;
use Doctrine\Inflector\InflectorFactory;
use Illuminate\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Connection;
use ReflectionClass;
use Throwable;

trait Support
{
    protected $container;

    public function __construct (Container $container = null) {
        $this->container = $container ?? new Container();
    }

    protected function makeDatasetEvent ($event) {
        return 'dataset.' . $this->type() . '.' . $event;
    }

    /**
     * Bind the event dispatcher
     *
     * @param \Illuminate\Contracts\Events\Dispatcher $dispatcher
     */
    public function setEventDispatcher (Dispatcher $dispatcher) {
        $this->container->instance('events', $dispatcher);
    }

    /**
     * Shoot exiting event if an event returns boolean false as response
     *
     * @param string $event
     * @param array  $parameters
     *
     * @return bool
     * @throws \Illuminate\Contracts\Container\BindingResolutionException
     */
    protected function exitOnEventResponse (string $event, array $parameters = []) : bool {
        $result = $this->fireEvent($event, $parameters);
        if (false === $result) {
            $this->fireEvent('exiting', [ 'event' => $this->makeDatasetEvent($event) ]);

            return false;
        }

        return true;
    }

    /**
     * Fire an event if registered the event dispatcher
     *
     * @param       $event
     * @param array $parameters
     *
     * @return bool
     * @throws \Illuminate\Contracts\Container\BindingResolutionException
     */
    protected function fireEvent ($event, array $parameters = []) : bool {
        if (!$this->container || !$this->container->bound('events')) {
            return true;
        }
        $name = $this->makeDatasetEvent($event);
        $parameters = array_merge([ 'class' => get_class($this) ], $parameters);
        $result = $this->container->make('events')->until($name, $parameters);

        return $result === false ? false : true;
    }

    /**
     * Get inflector instance
     */
    protected function inflector () : Inflector {
        return InflectorFactory::create()->build();
    }

    /**
     * Get table name of the class
     */
    protected function tableize () : string {
        return $this->inflector()->tableize(class_basename($this));
    }

    /**
     * Get the connection name to use for the class
     */
    protected function connection () {
        return 'default';
    }

    /**
     * Get the Manger's connection instance
     */
    public function db () : Connection {
        return Manager::connection($this->connection());
    }

    /**
     * Get the table name to read from
     */
    protected function table () : string {
        return $this->inflector()->pluralize($this->tableize());
    }

    /**
     * Get the directory of the class instance
     */
    protected function instanceDirectory () {
        return dirname((new ReflectionClass(static::class))->getFileName());
    }

    /**
     * Filename for the CSV file
     */
    protected function filename () : string {
        return sprintf('%s/%s.csv', $this->instanceDirectory(), $this->inflector()->pluralize($this->table()));
    }

    /**
     * File open mode
     * Can be 'r', 'r+', 'w', 'w+', 'a', 'a+'
     * Learn: https://stackoverflow.com/a/1466036/2190689
     */
    protected function fileOpenMode () : string {
        return 'w+';
    }

    /**
     * Delimiter for the CSV
     */
    protected function delimiterCharacter () : string {
        return ',';
    }

    /**
     * Enclosure for the CSV
     */
    protected function enclosureCharacter () : string {
        return '"';
    }

    /**
     * Escape character for the CSV
     */
    protected function escapeCharacter () : string {
        return '\\';
    }

    /**
     * An exception was raised. Log or do whatever wanted
     *
     * @param \Throwable $t
     */
    protected function raisedException (Throwable $t) : void {
        return;
    }
}