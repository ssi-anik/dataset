<?php

use Carbon\Carbon;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Schema\Blueprint;

class DatabaseStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();

        // cleanup the auto generated files
        $files = [
            __DIR__ . '/Providers/users.csv',
            __DIR__ . '/Providers/orders.csv',
        ];
        foreach ( $files as $file ) {
            if (file_exists($file)) {
                unlink($file);
            }
        }

        BaseDatabaseStorageProvider::$TYPE = 'writer';
        BaseDatabaseStorageProvider::$DB_MANAGER = false;
        BaseDatabaseStorageProvider::$EXIT_ON_ERROR = true;
        BaseDatabaseStorageProvider::$LIMIT = 50;
        BaseDatabaseStorageProvider::$FETCH_USING = 'cursor';
        BaseDatabaseStorageProvider::$CONDITION = false;
        BaseDatabaseStorageProvider::$JOINS = false;
        BaseDatabaseStorageProvider::$CUSTOM_BUILDER = false;
        BaseDatabaseStorageProvider::$ORDER_BY = false;
        BaseDatabaseStorageProvider::$ORDER_BY_DIRECTION = 'ASC';
        BaseDatabaseStorageProvider::$COLUMNS = [];
        BaseDatabaseStorageProvider::$HEADERS = [];
        BaseDatabaseStorageProvider::$CONNECTION = 'default';
        BaseDatabaseStorageProvider::$TABLE = '';
        BaseDatabaseStorageProvider::$FILENAME = '';
        BaseDatabaseStorageProvider::$DELIMITER = ',';
        BaseDatabaseStorageProvider::$ENCLOSE_CHARACTER = '"';
        BaseDatabaseStorageProvider::$EXCEPTION_RECEIVED = false;
        BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER = 0;
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'w+';
        BaseDatabaseStorageProvider::$HAS_FILE_WRITER = false;
    }

    protected function formatEventName ($name, $type = 'writer') {
        return parent::formatEventName($name, $type);
    }

    protected function rollbackDatabase () {
        $connections = array_keys($this->getDatabaseConnections());

        foreach ( $connections as $connection ) {
            $this->rollbackMigration($connection, 'users');
            $this->rollbackMigration($connection, 'categories');
            $this->rollbackMigration($connection, 'products');
            $this->rollbackMigration($connection, 'orders');
            $this->rollbackMigration($connection, 'order_details');
        }
    }

    protected function migrateDatabase () {
        $this->rollbackDatabase();
        $connections = array_keys($this->getDatabaseConnections());

        foreach ( $connections as $connection ) {
            $this->createTableMigration($connection, 'users', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('number');
                $table->string('email');
                $table->timestamps();
            });

            $this->createTableMigration($connection, 'categories', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('image_url');
                $table->string('slug');
            });

            $this->createTableMigration($connection, 'products', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('category_id');
                $table->string('name');
                $table->string('image_url');
                $table->smallInteger('price');
            });

            $this->createTableMigration($connection, 'orders', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('user_id');
                $table->string('order_uuid');
                $table->smallInteger('total_price');
                $table->timestamp('created_at');
            });

            $this->createTableMigration($connection, 'order_details', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('order_id');
                $table->smallInteger('product_id');
                $table->smallInteger('price');
            });
        }
    }

    protected function seedUserTable (array $config = []) {
        $rows = $config['rows'] ?? 30;
        $locale = $config['locale'] ?? 'en_US';
        $connection = $config['connection'] ?? 'default';
        $table = $config['table'] ?? 'users';
        $enclosure = $config['enclosure'] ?? false;

        $faker = $this->getFaker($locale);
        $now = Carbon::now();
        $dateVariations = [ 0, 5, 7, 15, 30, 100, 150, 180 ];
        /*$dates = [];
        foreach ( $dateVariations as $variation ) {
            $dates[] = (clone $now)->subDays($variation);
            // ->toDateTimeString();
        }*/

        $data = [];
        foreach ( range(1, $rows) as $i ) {
            $onDate = (clone $now)->subDays($faker->randomElement($dateVariations))
                                  ->subHours($faker->randomElement(range(0, 23)))
                                  ->subMinutes($faker->randomElement(range(0, 59)))
                                  ->subSeconds($faker->randomElement(range(0, 59)));
            // $onDate = $dates[array_rand($dates, 1)];
            $data[] = [
                'name'       => $enclosure ? sprintf('"%s"', $faker->name) : $faker->name,
                'email'      => $faker->companyEmail,
                'number'     => $faker->e164PhoneNumber,
                'created_at' => $onDate->toDateTimeString(),
                'updated_at' => $onDate->toDateTimeString(),
            ];
        }

        Manager::connection($connection)->table($table)->insert($data);

        return $data;
    }

    protected function seedCategoryTable (array $config = []) {
        $rows = $config['rows'] ?? 20;
        $locale = $config['locale'] ?? 'en_US';
        $connection = $config['connection'] ?? 'default';
        $table = $config['table'] ?? 'categories';

        $faker = $this->getFaker($locale);
        $data = [];
        foreach ( range(1, $rows) as $i ) {
            $data[] = [
                'name'      => $faker->lexify('Category - ??????'),
                'image_url' => $faker->imageUrl(),
                'slug'      => $faker->unique()->slug,
            ];
        }
        Manager::connection($connection)->table($table)->insert($data);

        return $data;
    }

    protected function seedProductTable (array $config = []) {
        $rows = $config['rows'] ?? 50;
        $categoryMin = $config['category_min'] ?? 1;
        $categoryMax = $config['category_max'] ?? 20;
        $locale = $config['locale'] ?? 'en_US';
        $connection = $config['connection'] ?? 'default';
        $table = $config['table'] ?? 'products';

        $faker = $this->getFaker($locale);
        $data = [];
        foreach ( range(1, $rows) as $i ) {
            $data[] = [
                'category_id' => $faker->randomElement(range($categoryMin, $categoryMax)),
                'name'        => $faker->lexify('Product - ??????'),
                'image_url'   => $faker->imageUrl(),
                'price'       => $faker->randomElement(range(50, 4000)),
            ];
        }
        Manager::connection($connection)->table($table)->insert($data);

        return $data;
    }

    protected function seedOrderTable (array $config = []) {
        $rows = $config['rows'] ?? 30;
        $userMin = $config['user_min'] ?? 1;
        $userMax = $config['user_max'] ?? 30;
        $productMin = $config['product_min'] ?? 1;
        $productMax = $config['product_max'] ?? 50;
        $locale = $config['locale'] ?? 'en_US';
        $connection = $config['connection'] ?? 'default';
        $table = $config['table'] ?? 'orders';
        $detailsTable = $config['details_table'] ?? 'order_details';

        $faker = $this->getFaker($locale);

        $orders = [];
        $details = [];
        $now = Carbon::now();
        $dateVariations = [ 0, 5, 7, 15, 30, 100, 150, 180 ];

        foreach ( range(1, $rows) as $i ) {
            $orders[] = [
                'user_id'     => $faker->randomElement(range($userMin, $userMax)),
                'order_uuid'  => $faker->unique()->uuid,
                'total_price' => $faker->randomFloat(2, 500, 5000),
                'created_at'  => (clone $now)->subDays($faker->randomElement($dateVariations))
                                             ->subHours($faker->randomElement(range(0, 23)))
                                             ->subMinutes($faker->randomElement(range(0, 59)))
                                             ->subSeconds($faker->randomElement(range(0, 59)))
                                             ->toDateTimeString(),
            ];

            foreach ( range(1, $faker->randomElement(range(3, 9))) as $j ) {
                $details[] = [
                    'order_id'   => $i,
                    'product_id' => $faker->randomElement(range($productMin, $productMax)),
                    'price'      => $faker->randomElement(range(50, 4000)),
                ];
            }
        }
        Manager::connection($connection)->table($table)->insert($orders);
        Manager::connection($connection)->table($detailsTable)->insert($details);

        return [
            'order_count'   => count($orders),
            'details_count' => count($details),
            'order_table'   => $table,
            'details_table' => $detailsTable,
            'orders'        => $orders,
            'order_details' => $details,
        ];
    }

    protected function getUserProvider () {
        return (new UserProvider($this->container));
    }

    protected function getOrderProvider () {
        return (new Order($this->container));
    }

    private function getStringLinesFrom ($file, $rows = 1, array $config = []) : array {
        $data = [];
        $length = $config['length'] ?? 2048;

        if (($handle = fopen($file, "r")) !== false) {
            do {
                if (($lines = fgets($handle, $length)) === false) {
                    break;
                }
                $data[] = $lines;
                if (--$rows <= 0) {
                    break;
                }
            } while ( true );

            fclose($handle);
        }

        return $data;
    }

    private function getRowsFrom ($file, $lines = 1, array $config = []) : array {
        $delimiter = $config['delimiter'] ?? ',';
        $enclosure = $config['enclosure'] ?? '"';
        $escape = $config['escape'] ?? '\\';
        $length = $config['length'] ?? 2048;

        $data = [];
        if (($handle = fopen($file, "r")) !== false) {
            do {
                if (($columns = fgetcsv($handle, $length, $delimiter, $enclosure, $escape)) === false) {
                    break;
                }
                $data[] = $columns;
                if (--$lines <= 0) {
                    break;
                }
            } while ( true );

            fclose($handle);
        }

        return $data;
    }

    private function getNthStringLineFrom ($file, $row = 1, array $config = []) : string {
        return $this->getStringLinesFrom($file, $row, $config)[$row - 1];
    }

    private function getNthRowFrom ($file, $line = 1, array $config = []) : array {
        return $this->getRowsFrom($file, $line, $config)[$line - 1];
    }

    public function testEventDispatcherIsWorking () {
        $this->addEventListener('test-true', function ($eventName) {
            return $eventName === 'test-true';
        });

        $this->addEventListener('test-count', function (...$payload) {
            return count($payload) === 3;
        });

        $this->assertTrue($this->dispatcher->until('test-true', 'test-true'));
        $this->assertTrue($this->dispatcher->until('test-count', [ 1, true, 'false' ]));
        $this->assertFalse($this->dispatcher->until('test-count'));
        $this->assertNull($this->dispatcher->until('no-event-listener'));
    }

    public function testProvidingEventDispatcherFromOutside () {
        $user = $this->getUserProvider();
        $user->setEventDispatcher($this->dispatcher);
        // return false exits program early
        $this->addEventListener($this->formatEventName('starting'), function () {
            return false;
        });

        // call the export method to start execution
        $result = $user->export();

        $this->assertFalse($result);
    }

    public function testCheckIfEventsAreFired () {
        // return false exits program early
        $this->addEventListener($this->formatEventName('starting'), function () {
            return false;
        });
        $received = false;

        // as returned false, it'll shoot exiting
        $this->addEventListener($this->formatEventName('exiting'), function () use (&$received) {
            $received = true;
        });

        // call the export method to start execution
        $result = $this->getUserProvider()->export();

        $this->assertFalse($result);
        $this->assertTrue($received);
    }

    public function testRenamingEventNames () {
        // change the type of the event
        $type = BaseDatabaseStorageProvider::$TYPE = 'writing-type';

        $receivedNewType = false;
        $this->addEventListener($this->formatEventName('starting', $type), function () use (&$receivedNewType) {
            $receivedNewType = true;
        });

        // return false exits program early
        $this->addEventListener($this->formatEventName('preparing_writer', $type), function () {
            return false;
        });

        // call the export method to start execution
        $result = $this->getUserProvider()->export();

        $this->assertFalse($result);
        $this->assertTrue($receivedNewType);
    }

    public function testDifferentTableName () {
        $table = 'new-user-table';
        $count = 15;

        $this->createTableMigration('default', $table, function (Blueprint $table) {
            $table->increments('id');
            $table->string('name');
            $table->string('number');
            $table->string('email');
            $table->timestamps();
        });

        $this->seedUserTable([ 'rows' => $count, 'table' => $table ]);

        BaseDatabaseStorageProvider::$TABLE = $table;

        $provider = $this->getUserProvider();

        $filename = $provider->filename();

        $result = $provider->export();
        $this->assertTrue($result);

        $firstLine = $this->getNthRowFrom($filename);
        // no of columns are 6 in users table
        $this->assertCount(6, $firstLine);

        $this->rollbackMigration('default', 'new-user-table');
        unlink($filename);
    }

    public function testDifferentCsvFilename () {
        $count = 15;
        $this->seedUserTable([ 'rows' => $count ]);
        // places file one directory up of the class file
        $filename = __DIR__ . '/new-file-for-users.csv';

        $filenameMatches = false;
        $this->addEventListener($this->formatEventName('preparing_writer'),
            function (...$payload) use ($filename, &$filenameMatches) {
                if (isset($payload[1]) && $payload[1] == $filename) {
                    $filenameMatches = true;
                }
            });

        BaseDatabaseStorageProvider::$FILENAME = $filename;

        $provider = $this->getUserProvider();

        $result = $provider->export();
        $this->assertTrue($filenameMatches);
        $this->assertTrue($result);

        $firstLine = $this->getNthRowFrom($filename);
        // no of columns are 6 in users table
        $this->assertFileExists($filename);
        $this->assertCount(6, $firstLine);

        // delete file
        unlink($filename);
    }

    public function testDifferentDatabaseConnection () {
        $this->seedUserTable([ 'connection' => 'sqlite' ]);

        BaseDatabaseStorageProvider::$CONNECTION = 'sqlite';
        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());

        $firstLine = $this->getNthRowFrom($filename = $provider->filename());
        // no of columns are 6 in users table
        $this->assertFileExists($filename);
        $this->assertCount(6, $firstLine);
    }

    public function testUseDifferentCsvDelimiter () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$DELIMITER = $delimiter = '|';
        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());

        $firstLine = $this->getNthStringLineFrom($filename = $provider->filename());
        // no of columns are 6 in users table
        $this->assertLessThanOrEqual(6, count(explode($delimiter, $firstLine)));
    }

    public function testUseChangedEncloseCharacter () {
        $this->seedUserTable([ 'rows' => 30, 'enclosure' => true, ]);
        BaseDatabaseStorageProvider::$ENCLOSE_CHARACTER = $enclosed = '!';
        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());

        $firstLine = $this->getNthStringLineFrom($filename = $provider->filename());
        // name, created_at, updated_at uses double quotes around - 3 * 2 = 6
        $this->assertSame(6, substr_count($firstLine, $enclosed));
    }

    public function testFileOpenMode () {
        $this->seedUserTable();
        // r+ also gives permission to write
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'a';

        $this->assertTrue($this->getUserProvider()->export());
    }

    public function testDifferentFileWriter () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$HAS_FILE_WRITER = true;

        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());
        file_put_contents($provider->filename(), $provider->getWriterContent());
        $firstRow = $this->getNthRowFrom($provider->filename());
        $this->assertCount(6, $firstRow, 'Matching count for first row');
    }

    public function testExceptionIsReceivedByMethod () {
        $this->seedUserTable();
        // in order to receive the exception, a file should be in the place
        // Because, in read mode it can't write to file
        // automatically deleted on tearDown method
        file_put_contents(__DIR__ . '/Providers/users.csv', '');
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'r';

        $this->assertFalse($this->getUserProvider()->export());
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER > 0);
    }

    public function testDoesNotExitOnFailureIfFalse () {
        $this->seedUserTable([ 'rows' => 10 ]);
        // in order to receive the exception, a file should be in the place
        // Because, in read mode it can't write to file
        // automatically deleted on tearDown method
        file_put_contents(__DIR__ . '/Providers/users.csv', '');

        BaseDatabaseStorageProvider::$LIMIT = 3;
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'r';
        BaseDatabaseStorageProvider::$EXIT_ON_ERROR = false;

        // false for exit on failure will return true
        $this->assertTrue($this->getUserProvider()->export());
        $this->assertSame(4, BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER);
    }

    public function testAddHeaderToCsv () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$HEADERS = $headers = [ 'name', 'email', 'number' ];
        $provider = $this->getUserProvider();

        $this->assertTrue($provider->export());

        $firstRow = $this->getNthRowFrom($provider->filename());
        $secondRow = $this->getNthRowFrom($provider->filename(), 2);
        $this->assertSame($firstRow, $headers);
        $this->assertCount(3, $secondRow);
    }

    public function testAddCustomHeaderToCsv () {
        $this->seedUserTable([ 'rows' => 15 ]);
        BaseDatabaseStorageProvider::$HEADERS = $headers = [
            'name'       => 'NaMe',
            'created_at' => 'Date of Joining',
            'email'      => 'emAIL',
            'number'     => 'No',
        ];
        $provider = $this->getUserProvider();

        $this->assertTrue($provider->export());

        $firstRow = $this->getNthRowFrom($provider->filename(), 1);
        $secondRow = $this->getNthRowFrom($provider->filename(), 2);
        $this->assertSame($firstRow, array_values($headers));
        $this->assertCount(4, $secondRow);
    }

    public function testAddExtraColumnThatDoesNotExistInDatabase () {
        $this->seedUserTable([ 'rows' => 15 ]);
        BaseDatabaseStorageProvider::$HEADERS = $headers = [
            'name'       => 'Name',
            'created_at' => 'Date of joining',
            'email'      => 'Email',
            'number'     => 'Number',
            'difference' => 'Number of days',
        ];
        $provider = $this->getUserProvider()->addMutation(function ($r) {
            return [
                'difference' => Carbon::now()->diffInDays(Carbon::createFromFormat('Y-m-d H:i:s', $r['created_at'])),
                'created_at' => Carbon::createFromFormat('Y-m-d H:i:s', $r['created_at'])->toDateString(),
            ];
        });

        $this->assertTrue($provider->export());

        $firstRow = $this->getNthRowFrom($provider->filename(), 1);
        $secondRow = $this->getNthRowFrom($provider->filename(), 2);
        $this->assertSame($firstRow, array_values($headers));
        $this->assertCount(5, $secondRow);
    }

    public function testFilterWhenHeaderIsNotPresent () {
        $this->seedUserTable([ 'rows' => 20 ]);
        $provider = $this->getUserProvider()->addFilter(function ($record) {
            return [
                'name'  => $record['name'],
                'email' => $record['email'],
            ];
        });

        $this->assertTrue($provider->export());

        $this->assertCount(2, $this->getNthRowFrom($provider->filename()));
    }

    public function testLimitingDatabaseQueryPerBatch () {
        $this->seedUserTable([ 'rows' => 20 ]);
        BaseDatabaseStorageProvider::$LIMIT = 3;
        $processedBatch = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function () use (&$processedBatch) {
            ++$processedBatch;
        });
        $this->getUserProvider()->export();
        $this->assertSame(7, $processedBatch);
    }

    public function testUseDatabaseCursorForPullingData () {
        $this->seedUserTable([ 'rows' => 20 ]);
        $isCursor = false;
        $this->addEventListener($this->formatEventName('iteration.started'), function (...$payload) use (&$isCursor) {
            $isCursor = isset($payload[1]) && $payload[1] == 'writer.cursor';

            // early exit, don't process further
            return false;
        });
        $this->getUserProvider()->export();
        $this->assertTrue($isCursor);
    }

    public function testUseBuilderChunkForPullingData () {
        $this->seedUserTable([ 'rows' => 20 ]);
        $isChunk = false;
        $this->addEventListener($this->formatEventName('iteration.started'), function (...$payload) use (&$isChunk) {
            $isChunk = isset($payload[1]) && $payload[1] == 'writer.chunk';

            // early exit, don't process further
            return false;
        });
        BaseDatabaseStorageProvider::$FETCH_USING = 'chunk';
        $this->getUserProvider()->export();
        $this->assertTrue($isChunk);
    }

    public function testFilterDataWithCondition () {
        $dbRows = $this->seedUserTable([ 'rows' => 100 ]);
        $diff = 30;
        $rowsPicked = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function (...$payload) use (&$rowsPicked) {
            $rowsPicked += $payload[2];
        });

        BaseDatabaseStorageProvider::$CONDITION = true;
        $provider = $this->getUserProvider()->addCondition(function ($q) use ($diff) {
            $q->where('created_at', '<=', Carbon::now()->subDays($diff)->toDateTimeString());
        });
        $provider->export();
        $acceptableRows = array_filter($dbRows, function ($item) use ($diff) {
            return Carbon::createFromFormat('Y-m-d H:i:s', $item['created_at'])->diffInDays(Carbon::now()) >= $diff;
        });

        $this->assertCount($rowsPicked, $acceptableRows);
    }

    public function testOrderByWithDirection () {
        $this->seedUserTable([ 'rows' => 100 ]);
        BaseDatabaseStorageProvider::$ORDER_BY = true;
        BaseDatabaseStorageProvider::$ORDER_BY_DIRECTION = 'DESC';
        $provider = $this->getUserProvider()->addOrderBy(function () {
            return Manager::connection()->raw('created_at');
        });
        $result = $provider->export();
        $this->assertTrue($result);
        $first = $this->getNthRowFrom($provider->filename())[4];
        $second = $this->getNthRowFrom($provider->filename(), 2)[4];

        $format = 'Y-m-d H:i:s';
        $this->assertTrue(Carbon::createFromFormat($format, $first)->gte(Carbon::createFromFormat($format, $second)));
    }

    public function testDatabaseConnectionUsingDbMethod () {
        $this->seedUserTable([ 'rows' => 20, 'connection' => 'sqlite' ]);
        BaseDatabaseStorageProvider::$DB_MANAGER = true;
        $rowCount = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function (...$payload) use (&$rowCount) {
            $rowCount += $payload[2];
        });
        $provider = $this->getUserProvider()->addDb(function () {
            return Manager::connection('sqlite');
        });
        $result = $provider->export();
        $this->assertTrue($result);
        $this->assertSame(20, $rowCount);
    }

    public function testColumnMethodPullsOnlySpecificFields () {
        $this->seedUserTable([ 'rows' => 20 ]);
        BaseDatabaseStorageProvider::$COLUMNS = [
            'name',
            'created_at',
            Manager::connection()->raw("'extra value' as extra_value"),
        ];
        $provider = $this->getUserProvider();

        $this->assertTrue($provider->export());
        $this->assertCount(3, $this->getNthRowFrom($provider->filename()));
    }

    public function testJoinFunction () {
        $connection = 'default';
        $this->seedUserTable();
        $this->seedCategoryTable();
        $this->seedProductTable();
        $data = $this->seedOrderTable();

        BaseDatabaseStorageProvider::$JOINS = true;
        BaseDatabaseStorageProvider::$ORDER_BY = true;
        BaseDatabaseStorageProvider::$HEADERS = [
            'order_uuid'     => 'Order UUID',
            'username'       => 'User\'s name',
            'product_name'   => 'Ordered product',
            'product_image'  => 'Product image',
            'product_price'  => 'Price of product on order',
            'category_name'  => 'Category',
            'category_image' => 'Category Image',
        ];
        BaseDatabaseStorageProvider::$COLUMNS = [
            Manager::connection($connection)->raw('orders.order_uuid'),
            Manager::connection($connection)->raw('users.name as username'),
            Manager::connection($connection)->raw("'----' as separator_one"),
            Manager::connection($connection)->raw('products.name as product_name'),
            Manager::connection($connection)->raw('products.image_url as product_image'),
            Manager::connection($connection)->raw('order_details.price as product_price'),
            Manager::connection($connection)->raw("'----' as separator_two"),
            Manager::connection($connection)->raw('categories.image_url as category_image'),
            Manager::connection($connection)->raw('categories.name as category_name'),
        ];

        $rowsPicked = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function (...$payload) use (&$rowsPicked) {
            $rowsPicked += $payload[2];
        });

        $provider = $this->getOrderProvider()->addOrderBy(function () use ($connection) {
            return Manager::connection($connection)->raw('orders.id');
        })->addJoin(function () {
            return [
                [
                    'order_details',
                    'order_details.order_id',
                    '=',
                    'orders.id',
                ],
                [
                    'products',
                    'order_details.product_id',
                    '=',
                    'products.id',
                ],
                [
                    'users',
                    'orders.user_id',
                    '=',
                    'users.id',
                ],
                [
                    'table'    => 'categories',
                    'first'    => 'products.category_id',
                    'operator' => '=',
                    'second'   => 'categories.id',
                    'type'     => 'inner',
                    'where'    => false,
                ],
            ];
        });
        $this->assertTrue($provider->export());

        $this->assertSame($rowsPicked, $data['details_count']);
        $nthRow = $this->getNthRowFrom($provider->filename(), 2);
        $this->assertCount(7, $nthRow);
        $this->assertStringStartsWith('Category -', $this->getNthRowFrom($provider->filename(), 2)[5]);
    }

    public function testQueryBuilderFunction () {
        $connection = 'default';
        $this->seedUserTable();
        $this->seedCategoryTable();
        $this->seedProductTable();
        $data = $this->seedOrderTable();

        BaseDatabaseStorageProvider::$HEADERS = $headers = [
            'order_uuid'     => 'OrderUUID',
            'username'       => 'User\'s name',
            'product_name'   => 'Ordered product',
            'product_image'  => 'Product image',
            'product_price'  => 'Price of product on order',
            'category_name'  => 'Category',
            'category_image' => 'Category Image',
        ];
        BaseDatabaseStorageProvider::$CUSTOM_BUILDER = true;

        $rowsPicked = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function (...$payload) use (&$rowsPicked) {
            $rowsPicked += $payload[2];
        });

        $now = Carbon::now();
        $beforeDate = (clone $now)->subDays(10);

        $provider = $this->getOrderProvider()->addBuilder(function () use ($connection, $beforeDate) : Builder {
            return Manager::connection($connection)
                          ->table('orders')
                          ->where(function (Builder $q) use ($beforeDate) {
                              return $q->where('orders.created_at', '<=', $beforeDate->toDateTimeString());
                          })
                          ->join('order_details', 'order_details.order_id', '=', 'orders.id')
                          ->join('products', 'order_details.product_id', '=', 'products.id')
                          ->join('users', 'orders.user_id', '=', 'users.id')
                          ->join('categories', 'products.category_id', '=', 'categories.id')
                          ->select([
                              Manager::connection($connection)->raw('orders.order_uuid'),
                              Manager::connection($connection)->raw('users.name as username'),
                              Manager::connection($connection)->raw("'----' as separator_one"),
                              Manager::connection($connection)->raw('products.name as product_name'),
                              Manager::connection($connection)->raw('products.image_url as product_image'),
                              Manager::connection($connection)->raw('order_details.price as product_price'),
                              Manager::connection($connection)->raw("'----' as separator_two"),
                              Manager::connection($connection)->raw('categories.image_url as category_image'),
                              Manager::connection($connection)->raw('categories.name as category_name'),
                          ]);

        });
        $this->assertTrue($provider->export());

        // get the order ids of the orders that were created before the $beforeDate
        $orderIds = [];
        foreach ( $data['orders'] as $idx => $order ) {
            if (Carbon::createFromFormat('Y-m-d H:i:s', $order['created_at'])->lte($beforeDate)) {
                $orderIds[] = $idx + 1;
            }
        }

        $validOrders = [];
        foreach ( $data['order_details'] as $details ) {
            if (in_array($details['order_id'], $orderIds)) {
                $validOrders[] = $details['order_id'];
            }
        }

        $this->assertCount($rowsPicked, $validOrders);

        $thirdRow = $this->getNthRowFrom($provider->filename(), 3);
        $this->assertStringStartsWith('Product -', $thirdRow[2]);
        $this->assertStringStartsWith('Category -', $thirdRow[5]);

        // build header, if the header contains space then it should be enclosed with the enclose character
        $stringHeader = implode(',', array_map(function ($column) {
            return strpos($column, ' ') !== false ? sprintf('"%s"', $column) : $column;
        }, $headers));

        $this->assertSame($stringHeader, trim($this->getNthStringLineFrom($provider->filename())));
    }
}
