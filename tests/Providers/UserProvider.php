<?php

class UserProvider extends BaseDatabaseStorageProvider
{
    protected function table () : string {
        return empty(static::$TABLE) ? 'users' : static::$TABLE;
    }

    public function filename () : string {
        return empty(static::$FILENAME) ? __DIR__ . '/users.csv' : static::$FILENAME;
    }
}
