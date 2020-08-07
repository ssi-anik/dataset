<?php

namespace Dataset;

use Closure;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use Illuminate\Support\Enumerable;
use League\Csv\Writer;
use Throwable;

abstract class DatabaseStorage
{
    use Support;

    /** @var $writer Writer */
    private $writer = null;

    /**
     * Type for the events
     */
    protected function type () : string {
        return 'writer';
    }

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
     *
     * @param array $record
     *
     * @return array
     */
    protected function mutation (array $record) : array {
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
     * @param Enumerable $records
     *
     * @param int        $page
     *
     * @return bool
     * @throws \Illuminate\Contracts\Container\BindingResolutionException
     */
    private function processRecordBatch (Enumerable $records, int $page) : bool {
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
                $this->raisedException($t);
                $this->fireEvent('exception', [
                    'error'   => $t,
                    'records' => $results,
                ]);
                if (false === $this->exitOnError()) {
                    return true;
                }
            }
        }

        // in case the records count is zero, nothing in the above was executed, thus bool(true)
        return $records->count() == 0 ? true : false;
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

        $csvColumns = [];
        // if user has provided the headers
        if ($headers) {
            // if the first element of the headers is integer, it's assumed to be integer based array
            $csvColumns = is_integer(array_keys($headers)[0]) ? $headers : array_keys($headers);
        }

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
            'uses'  => 'writer.cursor',
            'limit' => $this->limit(),
        ]);
        if (!$result) {
            return false;
        }

        $shouldBreak = false;
        $limit = $this->limit();
        $page = 1;
        $isCompleted = true;

        do {
            $offset = ($page - 1) * $limit;
            $builder = $this->getBuilder()->limit($limit)->offset($offset);
            $records = $builder->cursor();
            /**
             * if the result count is less than the limit. all the data are pulled
             */
            if ($records->count() < $limit) {
                $shouldBreak = true;
            }

            if (false === $this->processRecordBatch($records, $page++)) {
                $this->fireEvent('iteration.stopped', [
                    'uses' => 'writer.cursor',
                ]);
                $isCompleted = false;
                break;
            }
        } while ( false === $shouldBreak );

        $this->fireEvent('iteration.completed', [
            'uses'      => 'writer.cursor',
            'completed' => $isCompleted,
        ]);

        return $isCompleted;
    }

    /**
     * If you want `chunk` to be used
     */
    private function chunkBasedIteration () : bool {
        $result = $this->exitOnEventResponse('iteration.started', [
            'uses'  => 'writer.chunk',
            'limit' => $this->limit(),
        ]);
        if (!$result) {
            return false;
        }

        $response = $this->getBuilder()->chunk($this->limit(), function ($records, $page) {
            if (false === $this->processRecordBatch($records, $page)) {
                $this->fireEvent('iteration.stopped', [
                    'uses' => 'writer.chunk',
                ]);

                return false;
            }

            return true;
        });

        $this->fireEvent('iteration.completed', [
            'uses'      => 'writer.chunk',
            'completed' => $response,
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

        if (false === $this->prepareWriter()) {
            return false;
        }

        return $this->fetchUsing() == 'cursor' ? $this->cursorBasedIteration() : $this->chunkBasedIteration();
    }

    /**
     * Instantiate the file writer
     */
    private function prepareWriter () : bool {
        $result = $this->exitOnEventResponse('preparing_writer', [ 'file' => $this->filename() ]);
        if (!$result) {
            return false;
        }

        $this->writer = $this->getWriter();
        $this->addFileHeader();

        return true;
    }

    /**
     * Get the writer instance
     */
    protected function getWriter () : Writer {
        $writer = Writer::createFromPath($this->filename(), $this->fileOpenMode());
        $writer->setDelimiter($this->delimiterCharacter());
        $writer->setEnclosure($this->enclosureCharacter());
        $writer->setEscape($this->escapeCharacter());

        return $writer;
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
        $second = $join[3] ?? $join['second'];
        $type = $join[4] ?? $join['type'] ?? 'inner';
        $where = $join[5] ?? $join['where'] ?? false;

        return [ $table, $first, $operator, $second, $type, $where ];
    }
}