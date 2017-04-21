<?php namespace Dataset;

use ICanBoogie\Inflector;
use League\Csv\Reader;

abstract class Dataset
{
    use SQLHelper, Helper;

    /*
     * CSV RELATED PROPERTIES
     **/
    protected $source = '';
    protected $excludeHeader = true;
    protected $delimiter = ',';
    protected $enclosure = '"';
    protected $escape = '\\';
    protected $mapper = [];
    protected $headerAsTableField = false;
    protected $additionalFields = [];
    protected $ignoreCsvColumns = [];
    private $path = '';
    private $reader = null;

    /*
     * DATABASE RELATED PROPERTIES
     **/
    protected $table = '';
    private $query = '';
    private $connection = null;
    private $inflector = null;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->inflector = Inflector::get();
    }

    /*
     * CSV RELATED METHODS
     **/
    public function getAdditionalFields()
    {
        return (array)$this->additionalFields;
    }

    public function getPath()
    {
        return (string)$this->path;
    }

    public function getSource()
    {
        return (string)$this->source;
    }

    public function getDelimiter()
    {
        return (string)$this->delimiter;
    }

    public function getMapper()
    {
        return (array)$this->mapper;
    }

    public function getHeaderAsTableField()
    {
        return (bool)$this->headerAsTableField;
    }

    protected function getIgnoreCsvColumns()
    {
        return (array)$this->ignoreCsvColumns;
    }

    public function getExcludeHeader()
    {
        return (bool)$this->excludeHeader;
    }

    private function getReader()
    {
        return $this->reader;
    }

    public function getEnclosure()
    {
        return (string)$this->enclosure;
    }

    public function getEscape()
    {
        return (string)$this->escape;
    }

    private function setReader()
    {
        $this->reader = Reader::createFromPath($this->getSource());
        $this->reader->setDelimiter($this->getDelimiter())
                     ->setEnclosure($this->getEnclosure())
                     ->setEscape($this->getEscape());

        return $this;
    }

    public function import()
    {
        echo sprintf("\n-------------------------------- Importing From %s -----------------------------------------\n", get_class($this));
        // check if the source is empty
        if (empty(trim($this->source))) {
            $this->path = dirname((new \ReflectionClass(static::class))->getFileName());
            $this->source = $this->path . "/{$this->morphClassName()}.csv";
            // check if the file exists, existence moved from below to here, cause realpath checks the file existence
            if (!file_exists($this->source)) {
                throw new DatasetException("`{$this->source}` does not exist.");
            }
        } else {
            // a source is defined,
            // check the realpath of the source, false if does not exist
            // running php script from relative location and relative source will collide.
            $source = realpath($this->source);
            if (!$source) {
                // resolve the path via dir of the file
                $source = realpath(__DIR__ . "/" . $this->source);
                if (!$source) {
                    // file doesn't even exist, throw exception
                    throw new DatasetException("No file exists on `{$this->source}`.");
                } else {
                    // exists, add to source
                    $this->source = $source;
                }
            } else {
                // script ran from current location, relative path resolved.
                $this->source = $source;
            }
            // no exception was raised, get the path of the source.
            $this->path = dirname($this->source);
        }

        // check if table name exists, or generate
        if (empty($this->table)) {
            $this->table = $this->morphClassName();
        }

        // check if table exists in database, otherwise throw exception
        if (!$this->checkIfTableExists($this->table)) {
            throw new DatasetException("No table exists named `{$this->table}`.");
        }

        // check if header is not present and no mapper is available, throw exception in this case.
        if (false === $this->getHeaderAsTableField() && empty($this->getMapper())) {
            throw new DatasetException("Mapper must be present in absence of header.");
        }

        // check if constant fields exists, and not associative array
        if (!empty($this->getAdditionalFields()) && !$this->isMultidimensionalArray($this->getAdditionalFields())) {
            throw new DatasetException("Constant fields must be associative.");
        }

        // check if ignored csv column is flat array or not
		if ($this->isMultidimensionalArray($this->getIgnoreCsvColumns())) {
			throw new DatasetException("Ignored CSV Columns cannot be associative array.");
		}

        // file exists, set the reader
        $this->setReader();

        // get the csv columns, inside the columns, all the column name must be there
        // regardless of the database table entry
        $columns = [];
        // if the get header as table field is set, csv columns are those fields
        if ($this->getHeaderAsTableField()) {
            $csvColumns = array_map('trim', $this->getReader()->fetchOne());
            $columns = array_combine($csvColumns, $csvColumns);
        }

        // get the mapper by user
        $userMapped = $this->getMapper();
        if ($userMapped) {
            $columns = array_merge($columns, $userMapped);
        }

        // check if any columns is said to ignore/won't insert into database
        $ignoredColumns = $this->getIgnoreCsvColumns();
        if ($ignoredColumns) {
            $ignoredColumns = array_combine(array_values($ignoredColumns), array_fill(0, count($ignoredColumns), false));
            $columns = array_merge($columns, $ignoredColumns);
        }

        $mapper = [];
        // STRUCTURE: ['csv_column' => 'table_column', 'csv_column2' => false, 'csv_column3' => ['table_column3', function($row){ return 'result' }];
        // 1. ['user_name' => 'name', 'user_email' => 'email'];
        // 2. ['username' => 'name', 'password' => function($row){ return hash($row['password']); }]
        // 3. ['username' => 'name', 'password' => false, 'email', 'first_name' => function($row){ return $row['first_name'] . " " . $row['last_name'];}, 'last_name' => false];
        // 4. ['name', 'first_name', 'last_name', 'email'];
        // TABLE FIELDS VARIABLE STRUCTURE
        // [ 'csv_column' => ['table_column', null], 'csv_column3' => ['table_column3', function($row){ return 'result'; }]];
        foreach ($columns as $csvColumn => $value) {
            // Before set the key on variable, trim the column name
            if (is_string($value)) {
                $value = trim($value);
            }
            if (is_numeric($csvColumn) && is_string($value)) { // "EXAMPLE: 3, email", "EXAMPLE 4: FULL ARRAY"
                // cases: 1. FROM header, 2: From mapper as flat element
                // unset the element by key,
                // set the value as the new key
                unset($mapper[$csvColumn]);
                $mapper[$value] = [$value, null];
            } elseif (is_string($csvColumn) && is_string($value)) { // "EXAMPLE 3: username"
                $mapper[$csvColumn] = [$value, null];
            } elseif (is_string($csvColumn) && is_array($value)) { // "STRUCTURE: csv_column2"
                $mapper[$csvColumn] = $value;
            } elseif (is_string($csvColumn) && false === $value) { // "EXAMPLE: 3, password"
                $mapper[$csvColumn] = false;
                continue;
            } else {
                $message = sprintf('Invalid `%s` on %s::$mapper.', is_string($csvColumn) ? $csvColumn : (string)$value, get_class($this));
                throw new DatasetException($message);
            }
        }

        // EXCEPTIONS: ['csv_field' => false, 'csv_field2' => false]
        if (empty($mapper)) {
            throw new DatasetException("Nothing to import from CSV.");
        }
        // insertable table fields are going to be the fields that has NOT FALSE VALUES
        // 1. filter the values if any $mapper value has NOT FALSE values
        $filteredMap = array_filter($mapper);
        // 2. get the database table fields for those csv columns
        $tableColumns = array_map(function ($row) {
            return $row[0];
        }, $filteredMap);
        // 3. Merge those with the constant field values
        $insertAbleTableFields = array_merge(array_values($tableColumns), array_keys($this->getAdditionalFields()));
        // check if the table has fields
        $this->checkIfTableColumnsExist($insertAbleTableFields);
        // build the query with those values
        $this->query = $this->queryBuilder($insertAbleTableFields);

        // prepare the pdo statement
        $statement = $this->connection->getPDO()
									  ->prepare($this->query);

        $pagination = 100;
        $current = 0;
        $headerOffset = $this->getExcludeHeader() ? 1 : 0;
        $shouldContinue = true;
        $errorOccurred = false;
        do {
            $totalOffset = $current * $pagination + $headerOffset;
            $resultSet = $this->getReader()
                              ->setOffset($totalOffset)
                              ->setLimit($pagination)
                              ->fetchAssoc(array_keys($mapper));

            // increment the current page to +1
            ++$current;

            // should grab next chunk if the found data set greater than the pagination value
            // fetchAssoc returns an iterator
            $iterator_item_count = iterator_count($resultSet);
            if (0 === $iterator_item_count) {
                break;
            }
            echo sprintf("Loaded %5d%s %d rows.\n", $current, $this->inflector->ordinal($current), $iterator_item_count);

            // row counter for the result
            $onCurrentPageResultCount = 0;

            // loop over the result set
            foreach ($resultSet as $result) {
                // get the fields those are required to be taken from
                $matchedKeys = array_intersect_key(array_keys($filteredMap), array_keys($result));
                $values = [];
                ++$onCurrentPageResultCount;
                $currentRowNumber = $totalOffset + $onCurrentPageResultCount;
                $stopCurrentRow = false;
                foreach ($matchedKeys as $key) {
                    // ['csv_column' => ['table_column', 'transformer()']]; @ position 1
                    // transform values if required
                    if (is_callable($mapper[$key][1])) {
                        $returnedValue = call_user_func($mapper[$key][1], ...[
                            $result,
                            $currentRowNumber,
                        ]);
                        // user explicitly returned false from callback
                        if (false === $returnedValue) {
                            $stopCurrentRow = true;
                            $errorMessage = sprintf("`{$key}` from %s::\$mapper property explicitly returned bool(false)", get_class($this));
                            $this->errorAlert($errorMessage, $currentRowNumber, array_combine(array_slice($insertAbleTableFields, 0, count($values)), $values));
                            break;
                        }
                        $values[] = $returnedValue;
                    } else {
                        $values[] = $result[$key];
                    }
                }
                if (true === $stopCurrentRow) {
                    continue;
                }

                // check if the user wants to modify any value from current rows from constant fields
                foreach ($this->getAdditionalFields() as $tableField => $operation) {
                    // if the user has set callback in constant field
                    // call the function or set the value
                    if (is_callable($operation)) {
                        $returnedValue = call_user_func($operation, ...[
                            $result,
                            $currentRowNumber,
                        ]);
                        // check if user explicitly returned false
                        if (false === $returnedValue) {
                            $stopCurrentRow = true;
                            $errorMessage = sprintf("`{$tableField}` from %s::\$additionalFields property explicitly returned bool(false)", get_class($this));
                            $this->errorAlert($errorMessage, $currentRowNumber, array_combine(array_slice($insertAbleTableFields, 0, count($values)), $values));
                            break;
                        }
                        $values[] = $returnedValue;
                    } else {
                        $values[] = $operation;
                    }
                }
                if (true === $stopCurrentRow) {
                    continue;
                }

                if (!$statement->execute($values)) {
                    $this->errorAlert($statement->errorInfo()[2], $currentRowNumber, array_combine($insertAbleTableFields, $values));
                    $errorOccurred = true;
                    break;
                }
            }
            if ($errorOccurred) {
                break;
            }
        } while ($shouldContinue);
        echo sprintf("-------------------------------- Import Finished from %s --------------------------------\n", get_class($this));
        return $errorOccurred;
    }

    private function errorAlert($message = '', $row = 0, array $data = [])
    {
        if ($message) {
            echo sprintf("MESSAGE: %s\n", $message);
        }
        if ($row) {
            echo sprintf("CSV ROW: %d\n", $row);
        }
        if ($data) {
            echo sprintf("VALUES: %s\n", json_encode($data));
        }
        echo PHP_EOL;
    }
}