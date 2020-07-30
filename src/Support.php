<?php

namespace Dataset;

use Doctrine\Inflector\Inflector;
use Doctrine\Inflector\InflectorFactory;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Connection;

trait Support
{
    protected function inflector () : Inflector {
        return InflectorFactory::create()->build();
    }

    protected function tableize () : string {
        return $this->inflector()->tableize(get_class($this));
    }

    protected function connection () {
        return 'default';
    }

    public function db () : Connection {
        return Manager::connection($this->connection());
    }
}