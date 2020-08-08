<?php

use Dataset\DatabaseStorage;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use League\Csv\Writer;

class BaseDatabaseStorageProvider extends DatabaseStorage
{
    static $TYPE = 'writer';
    static $EXIT_ON_ERROR = true;
    static $LIMIT = 50;
    static $FETCH_USING = 'cursor';
    static $CONDITION = false;
    static $JOINS = false;
    static $CUSTOM_BUILDER = false;
    static $ORDER_BY = false;
    static $ORDER_BY_DIRECTION = 'ASC';
    static $COLUMNS = [];
    static $HEADERS = [];
    static $CONNECTION = 'default';
    static $TABLE = '';
    static $FILENAME = '';
    static $DELIMITER = ',';
    static $ESCAPE_CHARACTER = '\\';
    static $ENCLOSE_CHARACTER = '"';
    static $EXCEPTION_RECEIVED = false;
    static $HANDLED_EXCEPTION_COUNTER = 0;
    static $FILE_OPEN_MODE = 'w+';
    static $HAS_FILE_WRITER = false;

    private $condition = null;
    private $join = null;
    private $orderBy = null;
    private $mutate = null;
    private $builder = null;

    protected function type () : string {
        return empty(static::$TYPE) ? parent::type() : static::$TYPE;
    }

    // provides dynamically adding conditions function
    public function addCondition ($callback) {
        $this->condition = $callback;

        return $this;
    }

    protected function conditionThrough () {
        return $this->condition
            ? call_user_func_array($this->condition, [])
            : function ($q) {
            };
    }

    // provides dynamically adding join
    public function addJoin ($callback) {
        $this->join = $callback;

        return $this;
    }

    protected function joinThrough () {
        return $this->join ? call_user_func_array($this->condition, []) : [];
    }

    // provides dynamically adding orderby
    public function addOrderBy ($callback) {
        $this->orderBy = $callback;

        return $this;
    }

    protected function orderByThrough () {
        return $this->orderBy ? call_user_func_array($this->orderBy, []) : $this->db()->raw('id');
    }

    // provides dynamically adding mutation function
    public function addMutation ($callback) {
        $this->mutate = $callback;

        return $this;
    }

    protected function mutateThrough ($record) {
        return $this->mutate ? call_user_func_array($this->mutate, [ $record ]) : [];
    }

    // provides dynamically adding mutation function
    public function addBuilder ($callback) {
        $this->builder = $callback;

        return $this;
    }

    protected function builderThrough () {
        if ($this->builder) {
            return call_user_func_array($this->builder, []);
        }

        throw new \Exception('Builder is not implemented but called');
    }

    public function db () : Connection {
        return parent::db();
    }

    protected function condition () : Closure {
        return false === static::$CONDITION ? parent::condition() : $this->conditionThrough();
    }

    protected function joins () : array {
        return false === static::$JOINS ? parent::joins() : $this->joinThrough();
    }

    protected function orderBy () : Expression {
        return false === static::$ORDER_BY ? parent::orderBy() : $this->orderByThrough();
    }

    protected function orderDirection () : string {
        return static::$ORDER_BY_DIRECTION;
    }

    protected function columns () : array {
        return empty(static::$COLUMNS) ? parent::columns() : static::$COLUMNS;
    }

    protected function headers () : array {
        return static::$HEADERS;
    }

    protected function limit () : int {
        return static::$LIMIT;
    }

    protected function fetchUsing () : string {
        return static::$FETCH_USING;
    }

    protected function mutation (array $record) : array {
        return $this->mutateThrough($record);
    }

    protected function exitOnError () : bool {
        return static::$EXIT_ON_ERROR;
    }

    protected function getWriterFrom () : Writer {
        throw new \Exception('Unimplemented');
    }

    protected function getWriter () : Writer {
        return static::$HAS_FILE_WRITER ? $this->getWriterFrom() : parent::getWriter();
    }

    protected function getBuilder () : Builder {
        return static::$CUSTOM_BUILDER ? $this->builderThrough() : parent::getBuilder();
    }

    protected function connection () {
        return static::$CONNECTION;
    }

    protected function table () : string {
        return empty(static::$TABLE) ? parent::table() : static::$TABLE;
    }

    public function filename () : string {
        return empty(static::$FILENAME) ? parent::filename() : static::$FILENAME;
    }

    protected function fileOpenMode () : string {
        return static::$FILE_OPEN_MODE;
    }

    protected function delimiterCharacter () : string {
        return static::$DELIMITER;
    }

    protected function enclosureCharacter () : string {
        return static::$ENCLOSE_CHARACTER;
    }

    protected function escapeCharacter () : string {
        return static::$ESCAPE_CHARACTER;
    }

    protected function raisedException (Throwable $t) : void {
        var_dump($t->getMessage());
        static::$EXCEPTION_RECEIVED = true;
        ++static::$HANDLED_EXCEPTION_COUNTER;
    }
}