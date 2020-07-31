<?php

namespace Dataset;

use Closure;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use League\Csv\Writer;
use Throwable;

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
     * Column name for order by
     */
    protected function orderBy () : Expression {
        return $this->db()->raw('id');
    }

    /**
     * Order by direction
     * Can be 'ASC', 'DESC'
     */
    protected function orderDirection () : string {
        return 'ASC';
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
     * Exit insertion if any error occurs
     */
    protected function exitOnError () : bool {
        return true;
    }

    /**
     * Process records in batch
     *
     * @param iterable $records
     *
     * @param int      $page
     *
     * @return bool
     * @throws \Illuminate\Contracts\Container\BindingResolutionException
     */
    private function processRecordBatch (iterable $records, int $page) : bool {
        $result = $this->exitOnEventResponse('iteration.batch', [
            'batch' => $page,
            'count' => $records->count(),
            'limit' => $this->limit(),
        ]);
        if (!$result) {
            return false;
        }

        $results = [];
        foreach ( $records as $record ) {
            if ($result = $this->processRecord((array) $record)) {
                $results[] = $result;
            }
        }

        if (!empty($results)) {
            try {
                $this->writer->insertAll($results);

                return true;
            } catch ( Throwable $t ) {
                if (false === $this->exitOnError()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Process each record & insert into CSV
     *
     * @param array $record
     *
     * @return array
     */
    private function processRecord (array $record) : array {
        $record = array_merge($record, $this->mutation($record));

        $headers = $this->headers();
        // if the first element of the headers is integer, it's assumed to be integer based array
        $csvColumns = is_integer(array_keys($headers)[0]) ? $headers : array_keys($headers);

        return $this->extractColumnsForCsv($csvColumns, $record);
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
        return empty($required) ? $record
            : array_replace(array_flip($required), array_intersect_key($record, array_flip($required)));
    }

    /**
     * If you want `cursor` to be used
     */
    private function cursorBasedIteration () : bool {
        $result = $this->exitOnEventResponse('iteration.started', [
            'uses'   => 'cursor',
            'offset' => $this->offset,
            'limit'  => $this->limit(),
        ]);
        if (!$result) {
            return false;
        }

        $shouldBreak = false;
        $limit = $this->limit();
        $page = 1;
        do {
            $builder = $this->getBuilder()->limit($limit)->offset($this->offset);
            $records = $builder->cursor();
            /**
             * if the result count is less than the limit. all the data are pulled
             */
            if ($records->count() < $limit) {
                $shouldBreak = true;
            }

            if (false === $this->processRecordBatch($records, $page++)) {
                $this->fireEvent('iteration.stopped', [
                    'uses' => 'cursor',
                ]);

                return false;
            }

            $this->offset += $limit;

        } while ( false === $shouldBreak );

        $this->fireEvent('iteration.completed', [
            'uses' => 'cursor',
        ]);

        return true;
    }

    /**
     * If you want `chunk` to be used
     */
    private function chunkBasedIteration () : bool {
        $result = $this->exitOnEventResponse('iteration.started', [
            'uses'  => 'chunk',
            'limit' => $this->limit(),
        ]);
        if (!$result) {
            return false;
        }

        $response = $this->getBuilder()->chunk($this->limit(), function ($records, $page) {
            if (false === $this->processRecordBatch($records, $page)) {
                $this->fireEvent('iteration.stopped', [
                    'uses' => 'chunk',
                ]);

                return false;
            }
        });

        $this->fireEvent('iteration.completed', [
            'uses' => 'chunk',
        ]);

        return $response;
    }

    /**
     * Prepare all the task
     * Export the result set into a CSV file
     */
    public function export () : bool {
        $result = $this->exitOnEventResponse('starting');
        if (!$result) {
            return false;
        }

        $this->createWriter();
        $this->addFileHeader();

        return $this->fetchUsing() == 'cursor' ? $this->cursorBasedIteration() : $this->chunkBasedIteration();
    }

    /**
     * Instantiate the file writer
     */
    private function createWriter () : bool {
        $result = $this->exitOnEventResponse('creating', [ 'file' => $this->filename() ]);
        if (!$result) {
            return false;
        }

        $this->writer = Writer::createFromPath($this->filename(), $this->fileOpenMode());

        return true;
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
        $query = $this->db()
                      ->table($this->table())
                      ->where($this->condition())
                      ->select($this->columns())
                      ->orderBy($this->orderBy(), $this->orderDirection());

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