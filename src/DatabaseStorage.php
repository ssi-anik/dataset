<?php

namespace Dataset;

use Closure;
use Illuminate\Database\Query\Builder;
use League\Csv\Writer;

abstract class DatabaseStorage
{
    use Support;

    private $offset = 0;
    /** @var $writer Writer */
    private $writer = null;

    /**
     * Filter out the result
     */
    protected function condition () : Closure {
        return function ($q) {
        };
    }

    /**
     * Joins with the table
     */
    protected function joins () : array {
        return [];
    }

    /**
     * The columns to get from the table
     * If wants to fetch specific columns, return array of column names
     */
    protected function columns () : array {
        return [ '*' ];
    }

    /**
     * If wants to write headers in the csv file,
     * return the headers as array elements
     * Otherwise, the column names will be used
     */
    protected function headers () : array {
        return [];
    }

    /**
     * Number of rows to fetch at a time
     */
    protected function limit () : int {
        return 5000;
    }

    /**
     * Decide the technique to pull the data from storage
     * Can be either 'cursor' or 'chunk'
     *
     */
    protected function fetchUsing () : string {
        return 'cursor';
    }

    /**
     * Return new data based on any calculation or mutate data if required
     */
    protected function mutation ($record) : array {
        return [];
    }

    /**
     * Process each record & insert into CSV
     *
     * @param array $record
     *
     * @throws \League\Csv\CannotInsertRecord
     */
    private function processRecord (array $record) {
        $record = array_merge($record, $this->mutation($record));

        $headers = $this->headers();
        // if the first element of the headers is integer, it's assumed to be integer based array
        $csvColumns = is_integer(array_keys($headers)[0]) ? $headers : array_keys($headers);
        $storable = $this->extractColumnsForCsv($csvColumns, $record);
        $this->writer->insertOne($storable);
    }

    /**
     * Only extract the required fields to insert into the CSV
     *
     * @param array $required
     * @param array $record
     *
     * @return array
     */
    private function extractColumnsForCsv (array $required, array $record) : array {
        return array_replace(array_flip($required), array_intersect_key($record, array_flip($required)));
    }

    /**
     * If you want `cursor` to be used
     */
    private function cursorBasedIteration () {
        // fire event, if returns `false` explicitly, exit
        $result = $this->fireEvent('iteration', [
            'uses'   => 'cursor',
            'offset' => $this->offset,
            'limit'  => $this->limit(),
        ]);
        if (false === $result) {
            $this->fireEvent('exiting', [ 'event' => $this->eventName('iteration') ]);

            return;
        }

        $shouldBreak = false;
        $limit = $this->limit();
        do {
            $builder = $this->getBuilder()->limit($limit)->offset($this->offset);
            $results = $builder->cursor();
            /**
             * if the result count is less than the limit. all the data are pulled
             */
            if ($results->count() < $limit) {
                $shouldBreak = true;
            }
            foreach ( $results as $result ) {
                $this->processRecord((array) $result);
            }

            $this->offset += $limit;

        } while ( false === $shouldBreak );
    }

    /**
     * If you want `chunk` to be used
     */
    private function chunkBasedIteration () {
        // fire event, if explicitly
        $result = $this->fireEvent('iteration', [
            'uses'  => 'chunk',
            'limit' => $this->limit(),
        ]);
        if (false === $result) {
            $this->fireEvent('exiting', [ 'event' => $this->eventName('iteration') ]);

            return;
        }

        $this->getBuilder()->chunk($this->limit(), function ($record) {
            $record = (array) $record;
            $this->processRecord($record);
        });
    }

    /**
     * Prepare all the task
     * Export the result set into a CSV file
     */
    public function export () {
        $continue = $this->fireEvent('starting');
        if (false === $continue) {
            $this->fireEvent('exiting', [ 'event' => $this->eventName('starting') ]);

            return;
        }

        $this->createWriter();
        $this->addFileHeader();
        $this->fetchUsing() == 'cursor' ? $this->cursorBasedIteration() : $this->chunkBasedIteration();
    }

    /**
     * Instantiate the file writer
     */
    private function createWriter () : void {
        $continue = $this->fireEvent('creating', [ 'file' => $this->filename() ]);
        if (false === $continue) {
            $this->fireEvent('exiting', [ 'event' => $this->eventName('creating') ]);

            return;
        }
        $this->writer = Writer::createFromPath($this->filename(), $this->fileOpenMode());
    }

    /**
     * If you provided headers as non empty array
     */
    private function addFileHeader () : void {
        $headers = $this->headers();
        if (empty($headers)) {
            return;
        }

        $this->writer->insertOne($headers);
    }

    /**
     * Build the query
     * Overload if you want to build of your own
     *
     * @return Builder
     */
    protected function getBuilder () : Builder {
        $query = $this->db()->table($this->table())->where($this->condition())->select($this->columns());

        foreach ( $this->joins() as $join ) {
            $query->join(...$this->parseJoin($join));
        }

        return $query;
    }

    /**
     * Prepare the join query
     *
     * @param array $join
     *
     * @return array
     */
    private function parseJoin (array $join) : array {
        $table = $join[0] ?? $join['table'];
        $first = $join[1] ?? $join['first'];

        if ($first instanceof Closure) {
            return [ $table, $first, null, null, null ];
        }

        $operator = $join[2] ?? $join['operator'] ?? '=';
        $second = $join[3] ?? $join['second'] ?? null;
        if (is_null($second)) {
            if (strpos($first, $table) === 0) {
                $second = $this->table() . '.' . $this->inflector()->singularize($table) . '_' . 'id';
            } else {
                $second = $table . '.' . 'id';
            }
        }
        $type = $join[4] ?? $join['type'] ?? 'inner';
        $where = $join[5] ?? $join['where'] ?? false;

        return [ $table, $first, $operator, $second, $type, $where ];
    }

    public function data () {
        return [
            'table'   => $this->table(),
            'file'    => $this->filename(),
            'columns' => $this->columns(),
            'headers' => $this->headers(),
            'data'    => $this->getBuilder()->toSql(),
        ];
    }
}