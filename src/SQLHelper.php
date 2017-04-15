<?php namespace Dataset;

trait SQLHelper
{
	protected function queryBuilder ($table, array $fields) {
		$query = sprintf("INSERT INTO %s (%s) VALUES (%s)", $table, implode(", ", array_keys($fields)), implode(", ", array_fill(0, count($fields), "?")));

		return $query;
	}

	protected function checkIfTableExists ($tableName) {
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
}