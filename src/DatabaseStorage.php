<?php

namespace Dataset;

use Closure;
use Illuminate\Database\Query\Builder;

abstract class DatabaseStorage
{
    use Support;

    /**
     * Get the table name to read from
     */
    protected function table () : string {
        return $this->inflector()->pluralize($this->tableize());
    }

    /**
     * Filename for the CSV file
     */
    protected function filename () : string {
        return sprintf('%s.csv', $this->inflector()->pluralize($this->table()));
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
     * The columns to get from the table
     * If wants to fetch specific columns, return array of column names
     */
    public function columns () : array {
        return [ '*' ];
    }

    /**
     * Write headers in the csv file
     */
    protected function writeHeaders () : bool {
        return false;
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

    public function save () {
        $offset = 0;
        $shouldBreak = false;
        do {
            $builder = $this->getBuilder($offset);
            $shouldBreak = true;
            $offset += $this->limit();
        } while ( false === $shouldBreak );
    }

    private function getBuilder ($offset) : Builder {
        $query = $this->db()
                      ->table($this->table())
                      ->where($this->condition())
                      ->limit($this->limit())
                      ->offset($offset)
                      ->select($this->columns());

        foreach ( $this->joins() as $join ) {
            $query->join(...$this->parseJoin($join));
        }

        return $query;
    }

    private function parseJoin ($join) {
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
            'table'         => $this->table(),
            'file'          => $this->filename(),
            'columns'       => $this->columns(),
            'write_headers' => $this->writeHeaders(),
            'headers'       => $this->headers(),
            'data'          => $this->getBuilder(0)->toSql(),
        ];
    }
}