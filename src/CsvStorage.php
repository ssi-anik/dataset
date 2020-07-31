<?php

namespace Dataset;

use League\Csv\Reader;

abstract class CsvStorage
{
    use Support;

    /** @var $reader Reader */
    private $reader = null;

    /**
     * If the document contains any header, position of the header
     */
    protected function headerOffset () : ?int {
        return 0;
    }

    /**
     * If want to include the empty records in the result set
     */
    protected function skipEmptyRecord () : bool {
        return true;
    }

    /**
     * Array of stream filters
     * https://csv.thephpleague.com/9.0/connections/filters/#adding-a-stream-filter
     */
    protected function streamFilters () : array {
        return [];
    }

    /**
     * No of rows to be fetched at a time
     */
    protected function limit () : int {
        return 1000;
    }

    /**
     * If want to execute the insert operations in a transaction
     */
    protected function useTransaction () : bool {
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

    private function extractColumnsForDb (array $required, array $record) : array {
        return $record;
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
     * Prepare all the task
     * Import the result set into the database table
     */
    public function import () : bool {
        /*$result = $this->exitOnEventResponse('starting');
        if (!$result) {
            return false;
        }*/

        if (false === $this->prepareReader()) {
            return false;
        }

        $records = $this->reader->getRecords();
        foreach ( $records as $offset => $record ) {
            $this->dd($offset, $record);
            //$offset : represents the record offset
            //var_export($record) returns something like
            // array(
            //  'john',
            //  'doe',
            //  'john.doe@example.com'
            // );
            //
        }

        return true;
    }

    /**
     * Instantiate the file writer
     */
    private function prepareReader () : bool {
        /*$result = $this->exitOnEventResponse('reading', [ 'file' => $this->filename() ]);
        if (!$result) {
            return false;
        }*/

        $this->reader = Reader::createFromPath($this->filename(), $this->fileOpenMode());
        $this->reader->setDelimiter($this->delimiterCharacter());
        $this->reader->setEnclosure($this->enclosureCharacter());
        $this->reader->setEscape($this->escapeCharacter());
        $this->reader->setHeaderOffset($this->headerOffset());
        $this->skipEmptyRecord() ? $this->reader->skipEmptyRecords() : $this->reader->includeEmptyRecords();

        if ($this->reader->supportsStreamFilter() && ($filters = $this->streamFilters())) {
            foreach ( $filters as $filter ) {
                $this->reader->addStreamFilter($filter);
            }
        }

        return true;
    }
}