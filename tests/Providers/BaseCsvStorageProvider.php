<?php

use Dataset\CsvStorage;

class BaseCsvStorageProvider extends CsvStorage
{
    static $TYPE = '';
    static $HEADER_OFFSET = 0;
    static $SKIP_EMPTY = true;
    static $EXIT_ON_ERROR = true;
    static $STREAM_FILTERS = [];
    static $LIMIT = 20;
    static $USE_TRANSACTION = true;
    static $ENTRIES = [];
    static $FILTERS = [];
    static $HEADERS = [];
    static $MUTATION = [];
    static $CONNECTION = 'default';
    static $TABLE = '';
    static $FILENAME = '';
    static $DELIMITER = ',';
    static $EXCEPTION_RECEIVED = false;

    protected function type () : string {
        return empty(static::$TYPE) ? parent::type() : static::$TYPE;
    }

    protected function headerOffset () : ?int {
        return static::$HEADER_OFFSET;
    }

    protected function skipEmptyRecord () : bool {
        return static::$SKIP_EMPTY;
    }

    protected function streamFilters () : array {
        return static::$STREAM_FILTERS;
    }

    protected function limit () : int {
        return static::$LIMIT;
    }

    protected function useTransaction () : bool {
        return static::$USE_TRANSACTION;
    }

    protected function entries () : array {
        return empty(static::$ENTRIES) ? parent::entries() : static::$ENTRIES;
    }

    protected function filterInput (array $record) : array {
        return static::$FILTERS;
    }

    protected function headers () : array {
        return static::$HEADERS;
    }

    protected function mutation (array $record) : array {
        return static::$MUTATION;
    }

    protected function exitOnError () : bool {
        return static::$EXIT_ON_ERROR;
    }

    protected function connection () {
        return static::$CONNECTION;
    }

    protected function table () : string {
        return empty(static::$TABLE) ? parent::table() : static::$TABLE;
    }

    protected function filename () : string {
        return empty(static::$FILENAME) ? parent::filename() : static::$FILENAME;
    }

    public function delimiterCharacter () : string {
        return static::$DELIMITER;
    }

    protected function raisedException (Throwable $t) : void {
        static::$EXCEPTION_RECEIVED = true;
    }

}