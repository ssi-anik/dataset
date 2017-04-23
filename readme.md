## Dataset is a PHP package for importing data from csv to *SQL database

#### Installation
From your terminal, type `composer require anik/dataset`. You must have composer installed in your PC.

#### Examples
Examples are given in examples directory.

#### How to
Dataset has three classes. 
1. `Connection` - Responsible for Database connection.
2. `Dataset` - Responsible for data manipulation & import to your database.
3. `DatasetException` - Exceptions of this package.

##### Explanation
1. `Connection` - Connection has two public methods. 
    * `static getConnection` returns the Connection instance. You cannot instantiate this class without this one.
        - `getConnection` required parameters are - `host`, `database`, `username`, `password`. 
    * `getPDO` returns the pdo object
2. `Dataset` has few methods, you need to call `import` method to get your job done.
    - Properties 
        * `source` - Define your source/path of your CSV. If ignored, the class name will be converted to camel case and pluralized. `Brand` will be converted to `brands.csv`.
        * `delimiter` - Your CSV is delimited by. Default is `Comma (,)`.
        * `enclosure` - Your CSV data is enclosed with. Default is `Semicolon (")`.
        * `escape` - Default is is `Backslash (\)`
        * `table` - Table name you want to insert your data. If omitted,  the class name will be converted to camel case and pluralized. `Brand` will be converted to `brands`.
        * `excludeHeader` - Boolean value. Exclude the CSV header. Default is `true`. Set to `false` if there is no header.
        * `headerAsTableField` - Boolean value. Default `false`. If set to true means CSV header fields are the database table fields.
        * `ignoredCsvColumn` - CSV column you don't want to store in your table. One dimensional array holding the CSV column names as value. 
        * `additionalFields` - An array. Denotes some values will be provided from here those are not available in your CSV. Like created_at, updated_at, slug. You also can manipulate other data by passing closure to array key. Returning bool(false) from your closure will stop inserting that row.
            ```php
            [
              'slug' => function($row, $currentCSVRow) { return strtolower($row['name']); },
              'created_at' => date('Y-m-d h:i:s', strtotime('now')),
            ]
            ```
        * `mapper` - You want to manipulate your CSV column. An array must be passed. 
            ```php
            // format 1
            [
              'csv_column' => 'table_column',
            ]
            ```
            ```php
            // format 2
            [
              // same as putting this key to ignoredCsvColumn variable as a value
              'csv_column' =>  false, //If you don't want to store this column in your table 
            ] 
            ```
            ```php
            // format 3
            [
              'csv_column' => [
                  'table_column', 
                  function($row, $currentCSVRow){
                      // $row is the current row, as associative array
                      // $currentCSVRow is the current row number
                      // explicitly returning bool(false) will NOT insert current row into table
                      return "manipulated result"; 
                  },
              ]
            ]
            ```
            ```php
            // format 4
            // in case you don't have header and these are the TABLE fields
            [
              'csv_column_1', 
              'csv_column_2', 
              'csv_column_3', 
              'csv_column_4',
            ];
            ```
##### Notes
* When using closures, you can use `Connection`'s PDO to query in your database.
* All the columns you have in your CSV must be present. `ignoreCsvColumn` overwrites the `mapper` value. `mapper` value overwrites the `headerAsTableField`. 
    - Order is 
        1. First take the headers value if said `true`. - `headerAsTableField`.
        2. If any mapper is applied, apply on header fields. - `mapper`.
        3. If said to ignore the column - `ignoreCsvColumns`.