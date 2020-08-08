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

    private function getLinesFrom ($file, $lines = 1, array $config = []) : array {
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

    private function getNthLineFrom ($file, $line = 1, array $config = []) : array {
        return $this->getLinesFrom($file, $line, $config)[$line - 1];
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

        $firstLine = $this->getNthLineFrom($filename);
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

        $firstLine = $this->getNthLineFrom($filename);
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

        $firstLine = $this->getNthLineFrom($filename = $provider->filename());
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
        $firstRow = $this->getNthLineFrom($provider->filename());
        $this->assertTrue(6 === count($firstRow), 'Matching count for first row');
    }

    /*
    public function testDoNotSkipEmptyLines () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 5 ]);
        BaseDatabaseStorageProvider::$SKIP_EMPTY = false;

        $this->assertFalse($this->getCompanyProvider()->export());
    }

    public function testSkipEmptyLine () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER == 0);
    }

    public function testExceptionIsReceivedByMethod () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);
        BaseDatabaseStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER > 0);
    }

    public function testExitOnFailureIfFalse () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 2, 'lines' => 5 ]);
        BaseDatabaseStorageProvider::$SKIP_EMPTY = false;
        BaseDatabaseStorageProvider::$EXIT_ON_ERROR = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseDatabaseStorageProvider::$HANDLED_EXCEPTION_COUNTER == 2);
    }

    public function testExcludeCsvHeaderDealingWithProvidedHeader () {
        $count = 19;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseDatabaseStorageProvider::$HEADER_OFFSET = null;
        BaseDatabaseStorageProvider::$HEADERS = [ 'name', 'address', 'image_url' ];
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->export();

        $this->assertTrue(Manager::table('companies')->count() == $count + 1);
    }

    public function testExcludeCsvHeaderDealingZeroBasedIndex () {
        $count = 10;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseDatabaseStorageProvider::$HEADER_OFFSET = null;
        $this->getCompanyProvider()->addFilter(function ($record) {
            return [
                'name'      => $record[0],
                'slug'      => $record[0],
                'image_url' => $record[2],
            ];
        })->addMutation(function ($record) {
            return [];
        })->export();

        $this->assertTrue(Manager::table('companies')->count() == $count + 1);
    }

    public function testCustomHeader () {
        $this->generateCompaniesData();
        BaseDatabaseStorageProvider::$HEADERS = [ 'NAME', 'ADDRESS', 'IMAGE_URL' ];
        $result = $this->getCompanyProvider()->addFilter(function ($record) {
            return [
                'name'      => $record['NAME'],
                'slug'      => $record['slug'],
                'image_url' => $record['IMAGE_URL'],
            ];
        })->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->export();

        $this->assertTrue($result);
    }

    public function testLimitingCsvRows () {
        $this->generateCompaniesData([ 'lines' => 20 ]);
        BaseDatabaseStorageProvider::$LIMIT = 3;
        $processedBatch = 0;
        $this->addEventListener('dataset.reader.iteration.batch', function () use (&$processedBatch) {
            ++$processedBatch;
        });
        $this->getCompanyProvider()->export();
        $this->assertTrue($processedBatch == 7);
    }

    public function testDoNotUseDatabaseTransaction () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 10, 'lines' => 15 ]);

        BaseDatabaseStorageProvider::$SKIP_EMPTY = false;
        BaseDatabaseStorageProvider::$USE_TRANSACTION = false;

        $result = $this->getCompanyProvider()->export();
        $this->assertFalse($result);
        $this->assertTrue(10 == Manager::table('companies')->count());
    }

    public function testDatabaseTransactionNoInsertionOnFailure () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 7, 'lines' => 10 ]);
        BaseDatabaseStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(0 == Manager::table('companies')->count());
    }

    public function testMultipleTableEntries () {
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