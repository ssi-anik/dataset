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
	public function getPath () {
		return $this->path;
	}

	public function getSource () {
		return $this->source;
	}

	public function getDelimiter () {
		return $this->delimiter;
	}

	public function getMapper () {
		return $this->mapper;
	}

	private function getReader () {
		return $this->reader;
	}

	public function getEnclosure () {
		return $this->enclosure;
	}

	public function getEscape () {
		return $this->escape;
	}

	private function setReader () {
		$this->reader = Reader::createFromPath($this->getSource());
		$this->reader->setDelimiter($this->getDelimiter())
					 ->setEnclosure($this->getEnclosure())
					 ->setEscape($this->getEscape());

		return $this;
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

	public function import () {
		// check if the path is empty
		if ( empty(trim($this->source)) ) {
			$this->path = dirname(static::class);
			$this->source = $this->path . "/{$this->morphClassName()}.csv";
		}

		// check if the file exists
		if ( !file_exists($this->source) ) {
			throw new DatasetException("`{$this->source}` does not exist.");
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

		return $this->getReader()
					->getDelimiter();
	}
}