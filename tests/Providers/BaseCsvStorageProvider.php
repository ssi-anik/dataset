<?php

use Dataset\CsvStorage;
use League\Csv\Reader;

class BaseCsvStorageProvider extends CsvStorage
{
    static $TYPE = '';
    static $HEADER_OFFSET = 0;
    static $SKIP_EMPTY = true;
    static $EXIT_ON_ERROR = true;
    static $STREAM_FILTERS = [];
    static $LIMIT = 20;
    static $USE_TRANSACTION = true;
    static $ENTRIES = false;
    static $HEADERS = [];
    static $CONNECTION = 'default';
    static $TABLE = '';
    static $FILENAME = '';
    static $DELIMITER = ',';
    static $EXCEPTION_RECEIVED = false;
    static $HANDLED_EXCEPTION_COUNTER = 0;
    static $FILE_OPEN_MODE = 'r';
    static $HAS_FILE_READER = false;

    private $filter = null;
    private $mutate = null;
    private $entries = null;

    protected function type () : string {
        return empty(static::$TYPE) ? parent::type() : static::$TYPE;
    }

    protected function fileOpenMode () : string {
        return static::$FILE_OPEN_MODE;
    }

    // provides dynamically adding filter function
    public function addFilter ($callback) {
        $this->filter = $callback;

        return $this;
    }

    // provides dynamically adding mutation function
    public function addMutation ($callback) {
        $this->mutate = $callback;

        return $this;
    }

    // provides dynamically adding entries function
    public function addEntries ($callback) {
        $this->entries = $callback;

        return $this;
    }

    protected function filterThrough ($record) {
        return $this->filter ? call_user_func_array($this->filter, [ $record ]) : $record;
    }

    protected function mutateThrough ($record) {
        return $this->mutate ? call_user_func_array($this->mutate, [ $record ]) : [];
    }

    protected function entriesThrough () {
        return $this->entries ? call_user_func_array($this->entries, []) : [];
    }

    protected function getReaderFrom () : Reader {
        $reader = Reader::createFromString(<<<DATA
name,image_url,unnecessary
Libero Morbi Accumsan Foundation,http://placehold.it/350x150,1
Morbi Incorporated,http://placehold.it/350x150,2
Imperdiet Limited,http://placehold.it/350x150,3
Enim Sed Limited,http://placehold.it/350x150,extra_data
Leo Vivamus Consulting,http://placehold.it/350x150,extra_data
Feugiat Company,http://placehold.it/350x150,extra_data
Lobortis Consulting,http://placehold.it/350x150,extra_data
Nunc Pulvinar Incorporated,http://placehold.it/350x150,extra_data
Dolor Tempus Non PC,http://placehold.it/350x150,extra_data
Feugiat Tellus Lorem Company,http://placehold.it/350x150,extra_data
DATA
        );
        $reader->setHeaderOffset(0);

        return $reader;
    }

    protected function getReader () : Reader {
        return static::$HAS_FILE_READER ? $this->getReaderFrom() : parent::getReader();
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
        return false === static::$ENTRIES ? parent::entries() : $this->entriesThrough();
    }

    protected function filterInput (array $record) : array {
        return $this->filterThrough($record);
    }

    protected function headers () : array {
        return static::$HEADERS;
    }

    protected function mutation (array $record) : array {
        return $this->mutateThrough($record);
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
        ++static::$HANDLED_EXCEPTION_COUNTER;
    }
}