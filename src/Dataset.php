<?php namespace Dataset;

use ICanBoogie\Inflector;
use League\Csv\Reader;

abstract class Dataset
{
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
	protected $headerAsField = false;
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

	public function getHeaderAsField () {
		return (bool) $this->headerAsField;
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

	private function checkIfTableExists ($tableName) {
		$pdo = $this->database->getPDO();
		$query = "SHOW TABLES LIKE ?";
		$statement = $pdo->prepare($query);
		$statement->execute([ $tableName ]);
		if ( $statement->fetch() ) {
			return true;
		} else {
			return false;
		}
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

		// file exists, set the reader
		$this->setReader();

		// check if table name exists, or generate
		if ( empty($this->table) ) {
			$this->table = $this->morphClassName();
		}

		// check if table exists in database, otherwise throw exception
		if ( !$this->checkIfTableExists($this->table) ) {
			throw new DatasetException("No table exists named `{$this->table}`");
		}

		// check if the header should be excluded & header should be used as DB table column. Throw exception in this case.
		if ( $this->getExcludeHeader() && $this->getHeaderAsField() ) {
			throw new DatasetException("Header was excluded & used as table field.");
		}

		// check if header is not present and no mapper is available, throw exception in this case.
		if ( false === $this->getHeaderAsField() && empty($this->getMapper()) ) {
			throw new DatasetException("Mapper must be present in absence of header.");
		}

		$columns = [];
		// check which columns should be taken
		if ( $this->getHeaderAsField() ) {
			$columns = $this->getReader()
							->fetchOne();
		} else {
			$columns = $this->getMapper();
		}
		// if the columns are empty, in case got from the csv
		if ( empty($columns) ) {
			throw new DatasetException("Headers are not available.");
		}

		// get the columns user wants to insert.
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
			$tableFields = $columns;
		}

		return $tableFields;
	}
}