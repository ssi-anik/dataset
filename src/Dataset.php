<?php namespace Dataset;

use ICanBoogie\Inflector;
use League\Csv\Reader;

abstract class Dataset
{
	use SQLHelper;
	/*
	 * CSV RELATED PROPERTIES
	 **/
	private $path = '';
	protected $source = '';
	protected $excludeHeader = false;
	protected $delimiter = ',';
	protected $enclosure = '"';
	protected $escape = '\\';
	protected $mapper = [];
	private $reader = null;
	protected $stopOnError = false;
	protected $headerAsTableField = false;
	protected $constantFields = [];

	/*
	 * DATABASE RELATED PROPERTIES
	 **/
	protected $table = '';
	private $query = '';
	private $database = null;
	private $inflector = null;

	public function __construct (Database $database) {
		$this->database = $database;
		$this->inflector = Inflector::get();
	}

	/*
	 * CSV RELATED METHODS
	 **/
	public function getConstantFields () {
		return (array) $this->constantFields;
	}

	public function getPath () {
		return (string) $this->path;
	}

	public function getSource () {
		return (string) $this->source;
	}

	public function getDelimiter () {
		return (string) $this->delimiter;
	}

	public function getMapper () {
		return (array) $this->mapper;
	}

	public function getHeaderAsTableField () {
		return (bool) $this->headerAsTableField;
	}

	public function getExcludeHeader () {
		return (bool) $this->excludeHeader;
	}

	private function getReader () {
		return $this->reader;
	}

	public function getEnclosure () {
		return (string) $this->enclosure;
	}

	public function getEscape () {
		return (string) $this->escape;
	}

	private function setReader () {
		$this->reader = Reader::createFromPath($this->getSource());
		$this->reader->setDelimiter($this->getDelimiter())
					 ->setEnclosure($this->getEnclosure())
					 ->setEscape($this->getEscape());

		return $this;
	}

	public function getStopOnError () {
		return (bool) $this->stopOnError;
	}

	/*
	 * DATABASE RELATED METHODS
	 **/
	protected function morphClassName () {
		$extendedClass = get_class($this);
		$underScored = $this->inflector->underscore($extendedClass);

		return $this->inflector->pluralize($underScored);
	}

	/*
	 * Helper methods
	 **/
	private function isMultidimensionalArray ($array) {
		return ( array_values($array) !== $array );
	}

	public function import () {
		// check if the source is empty
		if ( empty(trim($this->source)) ) {
			$this->path = dirname(( new \ReflectionClass(static::class) )->getFileName());
			$this->source = $this->path . "/{$this->morphClassName()}.csv";
			// check if the file exists, existence moved from below to here, cause realpath checks the file existence
			if ( !file_exists($this->source) ) {
				throw new DatasetException("`{$this->source}` does not exist.");
			}
		} else {
			// a source is defined,
			// check the realpath of the source, false if does not exist
			// running php script from relative location and relative source will collide.
			$source = realpath($this->source);
			if ( !$source ) {
				// resolve the path via dir of the file
				$source = realpath(__DIR__ . "/" . $this->source);
				if ( !$source ) {
					// file doesn't even exist, throw exception
					throw new DatasetException("No file exists on `{$this->source}`");
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
		if ( empty($this->table) ) {
			$this->table = $this->morphClassName();
		}

		// check if table exists in database, otherwise throw exception
		if ( !$this->checkIfTableExists($this->table) ) {
			throw new DatasetException("No table exists named `{$this->table}`");
		}

		// check if the header should be excluded & header should be used as DB table column. Throw exception in this case.
		if ( $this->getExcludeHeader() && $this->getHeaderAsTableField() ) {
			throw new DatasetException("Header was excluded & used as table field.");
		}

		// check if header is not present and no mapper is available, throw exception in this case.
		if ( false === $this->getHeaderAsTableField() && empty($this->getMapper()) ) {
			throw new DatasetException("Mapper must be present in absence of header.");
		}

		// cannot use header as field and mapper
		if ( $this->getHeaderAsTableField() && !empty($this->getMapper()) ) {
			throw new DatasetException("Header and Mapper cannot be used together.");
		}

		// check if constant fields exists, and not associative array
		if ( !empty($this->getConstantFields()) && !$this->isMultidimensionalArray($this->getConstantFields()) ) {
			throw new DatasetException("Constant fields must be associative.");
		}


        // file exists, set the reader
        $this->setReader();

		// columns variable is actually the mapper or header
		$columns = [];
		// check which columns should be taken
		if ( $this->getHeaderAsTableField() ) {
			$columns = $this->getReader()
							->fetchOne();
		} else {
			$columns = $this->getMapper();
		}
		// if the columns are empty, in case got from the csv
		if ( empty($columns) ) {
			throw new DatasetException("Headers are not available.");
		}

		// get the columns user wants to insert into the table.
		$tableFields = [];
		if ( $this->isMultidimensionalArray($columns) ) {
			// STRUCTURE: ['csv_column' => 'table_column', 'csv_column2' => false, 'csv_column3' => function($row){ return 'result' }];
			// 1. ['user_name' => 'name', 'user_email' => 'email'];
			// 2. ['username' => 'name', 'password' => function($row){ return hash($row['password']); }]
			// 3. ['username' => 'name', 'password' => false, 'email' => 'email', 'first_name' => function($row){ return $row['first_name'] . " " . $row['last_name'];}, 'last_name' => false];
			foreach ( $columns as $csvColumn => $tableColumn ) {
				if ( is_numeric($csvColumn) ) {
					$tableFields[$tableColumn] = $tableColumn;
				} elseif ( $tableColumn ) {
					$tableFields[$csvColumn] = $tableColumn;
				}
			}
		} else {
			// 1. ['name', 'first_name', 'last_name', 'email'];
			$tableFields = array_combine($columns, $columns);
		}

		// merge the constant fields with the dynamic fields
		$insertAble = array_merge($tableFields, $this->getConstantFields());

		$this->query = $this->queryBuilder($this->table, $insertAble);
		$statement = $this->database->getPDO()
									->prepare($this->query);

		$pagination = 100;
		$current = 0;
		$headerOffset = $this->getExcludeHeader() ? 1 : 0;
		$shouldContinue = true;
		do {
			$totalOffset = $current * $pagination + $headerOffset;
			$resultSet = $this->getReader()
							  ->setOffset($totalOffset)
							  ->setLimit($pagination)
							  ->fetchAssoc(array_keys($tableFields));
			// increment the current page to +1
			++$current;

			// should grab next chunk if the found data set greater than the pagination value
			// fetchAssoc returns an iterator
			if ( iterator_count($resultSet) < $pagination ) {
				$shouldContinue = false;
			}

			// loop over the result set
			$onCurrentPageResultCount = 1;
			foreach ( $resultSet as $result ) {
				// get the fields those are required to be taken from
				$matchedKeys = array_intersect_key(array_keys($tableFields), array_keys($result));
				$values = [];
				foreach ( $matchedKeys as $key ) {
					// transform values if required
					if ( is_callable($tableFields[$key]) ) {
						$values[] = call_user_func($tableFields[$key], ...[
							$result,
							( $onCurrentPageResultCount + $totalOffset ),
						]);
					} else {
						$values[] = $result[$key];
					}
				}
				++$onCurrentPageResultCount;
				$values = array_merge($values, array_values($this->getConstantFields()));
				// TODO: csv_column = ['table_column' => transformer]
				$statement->execute($values);
			}
		} while ( $shouldContinue );

		return "Data inserted successfully.";
	}
}