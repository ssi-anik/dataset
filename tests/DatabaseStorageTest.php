<?php

use Carbon\Carbon;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Schema\Blueprint;

class DatabaseStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();

        // cleanup the auto generated files
        $files = [//            __DIR__ . '/Providers/users.csv',
        ];
        foreach ( $files as $file ) {
            if (file_exists($file)) {
                unlink($file);
            }
        }

        BaseDatabaseStorageProvider::$TYPE = 'writer';
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
                $table->string('uuid');
                $table->smallInteger('total_price');
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
        $dates = [];
        foreach ( $dateVariations as $variation ) {
            $dates[] = (clone $now)->subDays($variation)->startOfHour()->toDateTimeString();
        }

        $data = [];
        foreach ( range(1, $rows) as $i ) {
            $onDate = $dates[rand(0, count($dates) - 1)];
            // $onDate = $dates[array_rand($dates, 1)];
            $data[] = [
                'name'       => $enclosure ? sprintf('"%s"', $faker->name) : $faker->name,
                'email'      => $faker->companyEmail,
                'number'     => $faker->e164PhoneNumber,
                'created_at' => $onDate,
                'updated_at' => $onDate,
            ];
        }

        Manager::connection($connection)->table($table)->insert($data);

        return $data;
    }

    protected function getUserProvider () {
        return (new UserProvider($this->container));
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
        $this->assertTrue(is_null($this->dispatcher->until('no-event-listener')));
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

        $this->assertTrue(false === $result);
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

        $this->assertTrue(false === $result);
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

        $this->assertTrue(false === $result);
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
        $this->assertTrue(6 === count($firstLine));

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
        $this->assertTrue(file_exists($filename));
        $this->assertTrue(6 === count($firstLine));

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
        $this->assertTrue(file_exists($filename));
        $this->assertTrue(6 === count($firstLine));
    }

    public function testUseDifferentCsvDelimiter () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$DELIMITER = $delimiter = '|';
        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());

        $firstLine = $this->getNthStringLineFrom($filename = $provider->filename());
        // no of columns are 6 in users table
        $this->assertTrue(6 <= count(explode($delimiter, $firstLine)));
    }

    public function testUseChangedEncloseCharacter () {
        $this->seedUserTable([ 'rows' => 30, 'enclosure' => true, ]);
        BaseDatabaseStorageProvider::$ENCLOSE_CHARACTER = $enclosed = '!';
        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());

        $firstLine = $this->getNthStringLineFrom($filename = $provider->filename());
        // name, created_at, updated_at uses double quotes around - 3 * 2 = 6
        $this->assertTrue(6 === substr_count($firstLine, $enclosed));
    }

    public function testFileOpenMode () {
        $this->seedUserTable();
        // r+ also gives permission to write
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'r+';

        $this->assertTrue($this->getUserProvider()->export());
    }

    public function testDifferentFileWriter () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$HAS_FILE_WRITER = true;

        $provider = $this->getUserProvider();
        $this->assertTrue($provider->export());
        file_put_contents($provider->filename(), $provider->getWriterContent());
        $firstRow = $this->getNthRowFrom($provider->filename());
        $this->assertTrue(6 === count($firstRow), 'Matching count for first row');
    }

    public function testExceptionIsReceivedByMethod () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'r';

        $this->assertFalse($this->getUserProvider()->export());
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER > 0);
    }

    public function testDoesNotExitOnFailureIfFalse () {
        $this->seedUserTable([ 'rows' => 10 ]);

        BaseDatabaseStorageProvider::$LIMIT = 3;
        BaseDatabaseStorageProvider::$FILE_OPEN_MODE = 'r';
        BaseDatabaseStorageProvider::$EXIT_ON_ERROR = false;

        // false for exit on failure will return true
        $this->assertTrue($this->getUserProvider()->export());
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER == 4);
    }

    public function testAddHeaderToCsv () {
        $this->seedUserTable();
        BaseDatabaseStorageProvider::$HEADERS = $headers = [ 'name', 'email', 'number' ];
        $provider = $this->getUserProvider();

        $this->assertTrue($provider->export());

        $firstRow = $this->getNthRowFrom($provider->filename());
        $secondRow = $this->getNthRowFrom($provider->filename(), 2);
        $this->assertTrue($firstRow == $headers);
        $this->assertTrue(3 === count($secondRow));
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
        $this->assertTrue($firstRow == array_values($headers));
        $this->assertTrue(4 === count($secondRow));
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
        $this->assertTrue($firstRow == array_values($headers));
        $this->assertTrue(5 === count($secondRow));
    }

    public function testLimitingDatabaseQueryPerBatch () {
        $this->seedUserTable([ 'rows' => 20 ]);
        BaseDatabaseStorageProvider::$LIMIT = 3;
        $processedBatch = 0;
        $this->addEventListener($this->formatEventName('iteration.batch'), function () use (&$processedBatch) {
            ++$processedBatch;
        });
        $this->getUserProvider()->export();
        $this->assertTrue($processedBatch == 7);
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

        $this->assertTrue(count($acceptableRows) == $rowsPicked);
    }

    /*public function testMultipleTableEntries () {
        $this->generateMembersData([ 'lines' => 20, ]);
        BaseDatabaseStorageProvider::$ENTRIES = true;
        $this->getMemberProvider()->addEntries(function () {
            return [
                'members' => function (Model $model, array $record) {
                    $model->name = $record['name'];
                    $model->age = $record['age'];
                    $model->created_at = date('Y-m-d H:i:s');
                    $model->updated_at = date('Y-m-d H:i:s');
                    $model->save();

                    return $model;
                },
                'phones'  => function (Model $model, array $record, array $previous) {
                    $member = $previous['members'];

                    $model->member_id = $member->id;
                    $model->number = $record['phone'];
                    $model->save();

                    return $model;
                },
                'emails'  => function (Model $model, array $record, array $previous) {
                    $model->member_id = $previous['members']->id;
                    $model->email = $record['email'];
                    $model->save();

                    return $model;
                },
            ];
        })->export();

        $this->assertTrue(Manager::table('members')->count() == 20);
        $this->assertTrue(Manager::table('phones')->count() == 20);
        $this->assertTrue(Manager::table('emails')->count() == 20);
    }

    public function testMultipleTableEntriesCheckingDuplicateBeforeEntry () {
        $this->generateMembersData([
            'lines'             => 50,
            'duplication'       => true,
            'duplication_count' => 5,
        ]);
        BaseDatabaseStorageProvider::$ENTRIES = true;

        $this->getMemberProvider()->addEntries(function () {
            return [
                'members' => function (Model $model, array $record) {
                    // checks if any row exists with name,
                    $member = Manager::table('members')->where('name', $record['name'])->first();
                    if ($member) {
                        $model->id = $member->id;
                        $model->exists = true;
                    }

                    $model->name = $record['name'];
                    $model->age = $record['age'];
                    $model->created_at = date('Y-m-d H:i:s');
                    $model->updated_at = date('Y-m-d H:i:s');

                    $model->save();

                    return $model;
                },
                'phones'  => function (Model $model, array $record, array $previous) {
                    $member = $previous['members'];

                    $model->member_id = $member->id;
                    $model->number = $record['phone'];
                    $model->save();

                    return $model;
                },
                'emails'  => function (Model $model, array $record, array $previous) {
                    $model->member_id = $previous['members']->id;
                    $model->email = $record['email'];
                    $model->save();

                    return $model;
                },
            ];
        })->export();

        $this->assertTrue(Manager::table('members')->count() == 50);
        $this->assertTrue(Manager::table('phones')->count() == 55);
        $this->assertTrue(Manager::table('emails')->count() == 55);
    }*/
}