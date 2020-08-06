<?php

use Illuminate\Container\Container;
use Illuminate\Events\Dispatcher;
use PHPUnit\Framework\TestCase;

abstract class BaseTestClass extends TestCase
{
    protected $container, $dispatcher;

    protected function setUpContainer () {
        $this->container = new Container();
    }

    protected function setUpEventDispatcher () {
        $this->dispatcher = new Dispatcher($this->container);
    }

    protected function bindInContainer ($name, $concrete) {
        $this->container->bind($name, function () use ($concrete) {
            return $concrete;
        });
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
}